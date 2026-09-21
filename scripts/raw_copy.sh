#!/usr/bin/env bash
# Copia schemas raw_* entre bancos, tabela a tabela.
#
#   scripts/raw_copy.sh seed    raw_x [...]   analytics_dev     -> ingestion_sandbox
#   scripts/raw_copy.sh promote raw_x [...]   ingestion_sandbox -> analytics_dev
#   scripts/raw_copy.sh pull    raw_x [...]   analytics_prod    -> analytics_dev
#
# seed: antes de testar um pipeline incremental, para ele não baixar o histórico
# inteiro. promote: raw validada vai para o analytics_dev, onde o dbt a modela.
# pull: traz de prod o que o dbt de dev precisa ver (perfil DB__PROD__* do .env,
# que em dev pode ser uma credencial só de leitura).
#
# A cópia é TRUNCATE + COPY, nunca DROP: as views do dbt sobre a raw sobrevivem e
# o OID da tabela é estável.
#
# Cada tabela vai em duas etapas, para que nenhuma falha deixe o destino pela
# metade: a origem escreve o COPY num arquivo (se ela falhar, o destino nem foi
# tocado) e o destino carrega numa transação só -- TRUNCATE, COPY e a conferência
# do número de linhas, que derruba tudo se o que entrou não bater com o que a
# origem escreveu. Quem lê a tabela no destino espera o COMMIT em vez de ver
# tabela vazia. Custa um pico de disco em $TMP_DIR (RAW_COPY_TMP muda o lugar).
#
# Por tabela, decide sozinho entre cópia completa e só o delta:
#
#   sem tabela no destino .............. cria o DDL da origem e copia completa
#   sem loaded_at_utc nos dois lados ... completa
#   destino vazio ...................... completa
#   delta == total na origem ........... completa (é tabela de full refresh: ela
#                                        reescreve todas as linhas a cada carga)
#   destino tem índice único ........... completa (o delta exigiria upsert, e
#                                        essas tabelas são pequenas)
#   caso contrário ..................... só as linhas com loaded_at_utc > o
#                                        máximo do destino (fontes write: append
#                                        e as tabelas JSONB, que só inserem)
#
# O delta supõe que o destino é um espelho atrasado da origem e que loaded_at_utc
# só cresce. Se o destino tiver carga mais nova que a origem, ele diz "em dia"
# sem estar: use --full.
#
# A lista de colunas vem da ORIGEM e é usada nos dois lados, então os bancos
# precisam estar no mesmo nome de coluna: divergência dá erro de \copy, não
# cópia parcial silenciosa.
#
# --dry-run mostra a decisão de cada tabela sem mover dado.
# --full    ignora o delta e copia tudo, tabela a tabela.
#
# As credenciais saem do .env da raiz do repo, mas a variável de ambiente de mesmo
# nome vence o arquivo e dispensa o .env -- é por aí que a DAG raw_pull_prod (dev)
# chama o pull de dentro do container do Airflow.
set -euo pipefail

SANDBOX_DB=ingestion_sandbox
MODELS_DB=analytics_dev
ENV_FILE="$(cd "$(dirname "$0")/.." && pwd)/.env"
DRY_RUN=0
FORCA_FULL=0

# Onde ficam os arquivos intermediários de cada tabela. RAW_COPY_TMP aponta para
# outro disco quando /tmp for apertado: o pico é o tamanho do maior dump de texto
# (hoje ~1 GB, raw_camara.raw_camara_votos_deputados).
TMP_DIR=$(mktemp -d "${RAW_COPY_TMP:-${TMPDIR:-/tmp}}/raw_copy.XXXXXX")
trap 'rm -rf "$TMP_DIR"' EXIT

usage() {
    echo "uso: $0 {seed|promote|pull} [--dry-run] [--full] raw_<fonte> [raw_<fonte> ...]" >&2
    exit 1
}

# Valor de uma chave: a variável já exportada no ambiente vence o .env, e o .env
# é opcional. É assim que o Airflow roda este script -- no container não existe o
# .env do repo, só as DB__DEV__*/DB__PROD__* que o Astro injeta no ambiente.
env_opt() {
    local line
    [[ -n ${!1:-} ]] && {
        printf '%s' "${!1}"
        return 0
    }
    [[ -f $ENV_FILE ]] || return 0
    line=$(grep -E "^$1=" "$ENV_FILE" | tail -n1) || return 0
    line=${line#*=}
    line=${line%\"}
    line=${line#\"}
    printf '%s' "$line"
}

env_value() {
    local valor
    valor=$(env_opt "$1")
    [[ -n $valor ]] || {
        echo "faltou $1 no ambiente e em $ENV_FILE" >&2
        exit 1
    }
    printf '%s' "$valor"
}

[[ $# -ge 1 ]] || usage
acao=$1
shift
while [[ ${1:-} == --* ]]; do
    case $1 in
        --dry-run) DRY_RUN=1 ;;
        --full) FORCA_FULL=1 ;;
        *) usage ;;
    esac
    shift
done
[[ $# -ge 1 ]] || usage

DEV_HOST=$(env_value DB__DEV__HOST)
DEV_PORT=$(env_value DB__DEV__PORT)
DEV_USER=$(env_value DB__DEV__USER)
DEV_PASS=$(env_value DB__DEV__PASSWORD)

case "$acao" in
    seed)
        SRC_HOST=$DEV_HOST SRC_PORT=$DEV_PORT SRC_USER=$DEV_USER SRC_PASS=$DEV_PASS SRC_DB=$MODELS_DB
        DST_HOST=$DEV_HOST DST_PORT=$DEV_PORT DST_USER=$DEV_USER DST_PASS=$DEV_PASS DST_DB=$SANDBOX_DB
        ;;
    promote)
        SRC_HOST=$DEV_HOST SRC_PORT=$DEV_PORT SRC_USER=$DEV_USER SRC_PASS=$DEV_PASS SRC_DB=$SANDBOX_DB
        DST_HOST=$DEV_HOST DST_PORT=$DEV_PORT DST_USER=$DEV_USER DST_PASS=$DEV_PASS DST_DB=$MODELS_DB
        ;;
    pull)
        SRC_HOST=$(env_opt DB__PROD__HOST) SRC_PORT=$(env_opt DB__PROD__PORT)
        SRC_USER=$(env_opt DB__PROD__USER) SRC_PASS=$(env_opt DB__PROD__PASSWORD)
        SRC_DB=$(env_opt DB__PROD__NAME)
        DST_HOST=$DEV_HOST DST_PORT=$DEV_PORT DST_USER=$DEV_USER DST_PASS=$DEV_PASS DST_DB=$MODELS_DB
        faltando=()
        for chave in HOST PORT NAME USER PASSWORD; do
            [[ -n $(env_opt "DB__PROD__$chave") ]] || faltando+=("DB__PROD__$chave")
        done
        ((${#faltando[@]} == 0)) || {
            echo "pull exige ${faltando[*]} em $ENV_FILE" >&2
            echo "(em dev pode ser uma credencial só de leitura sobre os schemas raw_*)" >&2
            exit 1
        }
        ;;
    *) usage ;;
esac

origem() { PGPASSWORD="$SRC_PASS" psql -h "$SRC_HOST" -p "$SRC_PORT" -U "$SRC_USER" -d "$SRC_DB" -v ON_ERROR_STOP=1 "$@"; }
destino() { PGPASSWORD="$DST_PASS" psql -h "$DST_HOST" -p "$DST_PORT" -U "$DST_USER" -d "$DST_DB" -v ON_ERROR_STOP=1 "$@"; }
origem_dump() { PGPASSWORD="$SRC_PASS" pg_dump -h "$SRC_HOST" -p "$SRC_PORT" -U "$SRC_USER" -d "$SRC_DB" "$@"; }

for schema in "$@"; do
    [[ $schema =~ ^raw_[a-z0-9_]+$ ]] || {
        echo "schema recusado: '$schema' (só raw_<fonte>)" >&2
        exit 1
    }
    [[ $(origem -Atc "SELECT 1 FROM pg_namespace WHERE nspname = '$schema'") == 1 ]] || {
        echo "schema $schema não existe em $SRC_DB" >&2
        exit 1
    }
done

echo "$SRC_DB@$SRC_HOST -> $DST_DB@$DST_HOST: $*$([[ $FORCA_FULL == 1 ]] && echo '  (--full)')$([[ $DRY_RUN == 1 ]] && echo '  (dry-run)')"

# Bloco que derruba a transação do destino quando o que entrou não bate com o que
# a origem escreveu. É o que pega o stream cortado no meio: sem ele o destino
# aceitaria o pedaço que chegou e daria COMMIT feliz.
confere() {
    local alvo=$1 esperado=$2 expressao=$3
    cat <<SQL
DO \$conf\$
DECLARE entraram bigint;
BEGIN
    SELECT $expressao INTO entraram;
    IF entraram <> $esperado THEN
        RAISE EXCEPTION 'copia de $alvo: a origem escreveu $esperado linha(s), o destino recebeu %', entraram;
    END IF;
END
\$conf\$;
SQL
}

# Copia uma tabela em duas etapas, para que nenhuma falha deixe o destino pela
# metade. Com $marca é delta (só as linhas mais novas); sem, é cópia completa.
#
#   1. a origem escreve o COPY num arquivo. Se ela falhar -- erro, queda de rede,
#      kill --, o destino sequer foi tocado e o set -e aborta aqui;
#   2. o destino carrega numa transação só: TRUNCATE (na completa), COPY e a
#      conferência das linhas. Qualquer erro dá ROLLBACK e a tabela continua como
#      estava; o dbt só enxerga o conteúdo novo no COMMIT.
#
# O preço é um pico de disco em $TMP_DIR e o ACCESS EXCLUSIVE do TRUNCATE valendo
# até o COMMIT: quem lê a tabela espera, em vez de ver tabela vazia.
copia() {
    local schema=$1 tabela=$2 cols=$3 marca=${4:-}
    local arquivo="$TMP_DIR/$schema.$tabela" linhas

    if [[ -n $marca ]]; then
        origem -q -c "\copy (SELECT $cols FROM $schema.$tabela WHERE loaded_at_utc > '$marca') TO '$arquivo' (FORMAT text)"
    else
        origem -q -c "\copy $schema.$tabela ($cols) TO '$arquivo' (FORMAT text)"
    fi
    # FORMAT text escapa a quebra de linha, então uma linha do arquivo é uma linha
    # da origem -- inclusive nas tabelas JSONB.
    linhas=$(wc -l <"$arquivo")

    if [[ -n $marca ]]; then
        destino -q -1 \
            -c "CREATE TEMP TABLE _antes ON COMMIT DROP AS SELECT count(*) AS n FROM $schema.$tabela" \
            -c "\copy $schema.$tabela ($cols) FROM '$arquivo' (FORMAT text)" \
            -c "$(confere "$schema.$tabela" "$linhas" \
                  "(SELECT count(*) FROM $schema.$tabela) - (SELECT n FROM _antes)")"
    else
        destino -q -1 \
            -c "TRUNCATE TABLE $schema.$tabela" \
            -c "\copy $schema.$tabela ($cols) FROM '$arquivo' (FORMAT text)" \
            -c "$(confere "$schema.$tabela" "$linhas" \
                  "(SELECT count(*) FROM $schema.$tabela)")"
    fi
    rm -f "$arquivo"
}

for schema in "$@"; do
    [[ $DRY_RUN == 1 ]] || destino -q -c "CREATE SCHEMA IF NOT EXISTS $schema"
    tabelas=$(origem -Atc "SELECT tablename FROM pg_tables WHERE schemaname = '$schema' ORDER BY tablename")
    [[ -n $tabelas ]] || { echo "  $schema: nenhuma tabela na origem"; continue; }

    while read -r tabela; do
        [[ -n $tabela ]] || continue
        existe=$(destino -Atc "SELECT 1 FROM pg_tables WHERE schemaname='$schema' AND tablename='$tabela'")
        motivo="" nova=0
        if [[ $existe != 1 ]]; then
            nova=1 motivo="tabela nova no destino"
        fi

        if [[ $nova == 1 ]]; then
            if [[ $DRY_RUN == 1 ]]; then
                echo "  $schema.$tabela: completa ($motivo)"
                continue
            fi
            origem_dump -s -t "$schema.$tabela" --no-owner | destino -q -1 >/dev/null
        fi

        cols=$(origem -Atc "SELECT string_agg(quote_ident(column_name), ',' ORDER BY ordinal_position)
                            FROM information_schema.columns
                            WHERE table_schema='$schema' AND table_name='$tabela'")
        # O delta precisa de loaded_at_utc nos DOIS lados: a marca vem do destino e o
        # filtro roda na origem.
        conta_marca="SELECT count(*) FROM information_schema.columns
                     WHERE table_schema='$schema' AND table_name='$tabela'
                       AND column_name='loaded_at_utc'"
        tem_marca=0
        [[ $(destino -Atc "$conta_marca") == 1 && $(origem -Atc "$conta_marca") == 1 ]] && tem_marca=1
        marca=""
        [[ $tem_marca == 1 ]] && marca=$(destino -Atc "SELECT coalesce(max(loaded_at_utc)::text,'') FROM $schema.$tabela")

        if [[ -z $motivo && $FORCA_FULL == 1 ]]; then
            motivo="--full"
        fi
        if [[ -z $motivo ]]; then
            if [[ $tem_marca != 1 ]]; then
                motivo="sem loaded_at_utc nos dois lados"
            elif [[ -z $marca ]]; then
                motivo="destino vazio"
            else
                total=$(origem -Atc "SELECT count(*) FROM $schema.$tabela")
                delta=$(origem -Atc "SELECT count(*) FROM $schema.$tabela WHERE loaded_at_utc > '$marca'")
                unicos=$(destino -Atc "SELECT count(*) FROM pg_index WHERE indrelid='$schema.$tabela'::regclass AND indisunique")
                if [[ $delta == 0 ]]; then
                    echo "  $schema.$tabela: em dia ($total linha(s))"
                    continue
                elif [[ $delta == "$total" ]]; then
                    motivo="full refresh na origem ($total linha(s))"
                elif [[ $unicos != 0 ]]; then
                    motivo="tem índice único, delta exigiria upsert ($total linha(s))"
                fi
            fi
        fi

        if [[ -n $motivo ]]; then
            echo "  $schema.$tabela: completa -- $motivo"
            [[ $DRY_RUN == 1 ]] || copia "$schema" "$tabela" "$cols"
        else
            echo "  $schema.$tabela: delta de $delta linha(s) desde $marca"
            [[ $DRY_RUN == 1 ]] || copia "$schema" "$tabela" "$cols" "$marca"
        fi
        [[ $DRY_RUN == 1 ]] || destino -q -c "ANALYZE $schema.$tabela"
    done <<<"$tabelas"

    [[ $DRY_RUN == 1 ]] || destino -Atc "
        SELECT '  $schema: ' || count(*) || ' tabela(s) no destino'
        FROM pg_tables WHERE schemaname = '$schema'"
done
