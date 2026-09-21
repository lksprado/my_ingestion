#!/usr/bin/env bash
#
# Remove as tabelas de domínio do legislativo que sobraram na raw. Elas são
# resíduo de um caminho antigo: hoje esses dados são seeds do dbt
# (`ref('seed_senado_tipos_*')`, `ref('seed_camara_tipos_proposicao')`), gerados
# por pipelines/legislativo/_params/dbt_seed_maker.py, e nenhuma tabela dessas
# tem consumidor. Enquanto existirem, o raw_copy.sh só as copia por inteiro
# (não têm loaded_at_utc), o que é seguro mas inútil.
#
#   uso: scripts/drop_tabelas_orfas.sh {models|prod} [--confirmar]
#
#     models   analytics_dev   (perfil DB__DEV__*)
#     prod     produção        (perfil DB__PROD__*)
#
# **Sem --confirmar ele não derruba nada**: só mostra o que faria. É o inverso
# do loaded_at_migra.sh de propósito — rename se desfaz, DROP não.
#
# Antes de derrubar cada tabela, checa duas coisas e pula a tabela se qualquer
# uma falhar:
#
#   1. nenhum objeto depende dela (view do dbt, regra). Nunca usa CASCADE: se
#      alguém passou a ler a tabela, o certo é o script recusar, não arrastar
#      a view junto;
#   2. o seed correspondente existe em SEEDS_ROOT e tem pelo menos tantas
#      linhas quanto a tabela — ou seja, o dado continua em algum lugar.
set -euo pipefail

ENV_FILE="$(cd "$(dirname "$0")/.." && pwd)/.env"
MODELS_DB=analytics_dev
CONFIRMAR=0

# schema|tabela|seed correspondente em SEEDS_ROOT
ORFAS=(
    "raw_senado|raw_senado_tipos_decisao|seed_senado_tipos_decisao.csv"
    "raw_senado|raw_senado_tipos_entes|seed_senado_tipos_entes.csv"
    "raw_senado|raw_senado_tipos_projetos|seed_senado_tipos_projetos.csv"
    "raw_camara|raw_camara_tipos_proposicao|seed_camara_tipos_proposicao.csv"
)

usage() {
    echo "uso: $0 {models|prod} [--confirmar]" >&2
    exit 1
}

# Valor de uma chave do .env; vazio quando a chave não existe.
env_opt() {
    local line
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
        echo "faltou $1 em $ENV_FILE" >&2
        exit 1
    }
    printf '%s' "$valor"
}

[[ $# -ge 1 ]] || usage
alvo=$1
shift
while [[ ${1:-} == --* ]]; do
    case $1 in
        --confirmar) CONFIRMAR=1 ;;
        *) usage ;;
    esac
    shift
done
[[ $# -eq 0 ]] || usage

case "$alvo" in
    models)
        HOST=$(env_value DB__DEV__HOST) PORT=$(env_value DB__DEV__PORT)
        USER=$(env_value DB__DEV__USER) PASS=$(env_value DB__DEV__PASSWORD)
        DB=$MODELS_DB
        ;;
    prod)
        HOST=$(env_value DB__PROD__HOST) PORT=$(env_value DB__PROD__PORT)
        USER=$(env_value DB__PROD__USER) PASS=$(env_value DB__PROD__PASSWORD)
        DB=$(env_value DB__PROD__NAME)
        ;;
    *) usage ;;
esac

SEEDS_ROOT=$(env_value SEEDS_ROOT)
SEEDS_ROOT=${SEEDS_ROOT/#\~/$HOME}

banco() { PGPASSWORD="$PASS" psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -v ON_ERROR_STOP=1 "$@"; }

echo "$DB@$HOST$([[ $CONFIRMAR == 1 ]] || echo '  (simulação — use --confirmar para derrubar)')"

derrubadas=0 puladas=0 ausentes=0

for linha in "${ORFAS[@]}"; do
    IFS='|' read -r schema tabela seed <<<"$linha"
    qualificada="$schema.$tabela"

    existe=$(banco -Atc "SELECT count(*) FROM pg_tables
                         WHERE schemaname = '$schema' AND tablename = '$tabela'")
    if [[ $existe != 1 ]]; then
        echo "  $qualificada: não existe aqui"
        ausentes=$((ausentes + 1))
        continue
    fi

    linhas=$(banco -Atc "SELECT count(*) FROM $qualificada")

    # Views/regras que leem a tabela. Sem isso o DROP falharia (bom) ou, com
    # CASCADE, levaria a view junto (péssimo) — então o script decide antes.
    dependentes=$(banco -Atc "
        SELECT coalesce(string_agg(DISTINCT v.relname, ', '), '')
        FROM pg_depend d
        JOIN pg_rewrite r ON r.oid = d.objid
        JOIN pg_class v ON v.oid = r.ev_class
        WHERE d.refobjid = '$qualificada'::regclass
          AND d.deptype = 'n' AND v.oid <> '$qualificada'::regclass")
    if [[ -n $dependentes ]]; then
        echo "  $qualificada: PULADA — tem dependente(s): $dependentes"
        puladas=$((puladas + 1))
        continue
    fi

    # O dado tem de continuar existindo como seed do dbt.
    arquivo="$SEEDS_ROOT/$seed"
    if [[ ! -f $arquivo ]]; then
        echo "  $qualificada: PULADA — seed $seed não encontrada em $SEEDS_ROOT"
        puladas=$((puladas + 1))
        continue
    fi
    no_seed=$(($(wc -l <"$arquivo") - 1)) # menos o cabeçalho
    if ((no_seed < linhas)); then
        echo "  $qualificada: PULADA — a tabela tem $linhas linha(s) e o seed $seed" \
            "só $no_seed; o dado não está todo preservado"
        puladas=$((puladas + 1))
        continue
    fi

    echo "  $qualificada: $linhas linha(s), sem dependentes, $no_seed no seed $seed"
    if [[ $CONFIRMAR == 1 ]]; then
        banco -q -c "DROP TABLE $qualificada"
        echo "    🗑️  derrubada"
    fi
    derrubadas=$((derrubadas + 1))
done

if [[ $CONFIRMAR == 1 ]]; then
    echo "$derrubadas derrubada(s), $puladas pulada(s), $ausentes ausente(s)"
else
    echo "$derrubadas a derrubar, $puladas pulada(s), $ausentes ausente(s) — nada foi alterado"
fi
