#!/usr/bin/env bash
# Renomeia data_carga -> loaded_at_utc e acerta o DEFAULT para UTC, em todos os
# schemas raw_* de um banco (inclusive o `ingested_at` das tabelas de controle,
# que mantém o nome e muda só o fuso). Migração de uma vez só; depois dela o script vira
# no-op (a core faz o mesmo reparo sozinha na carga seguinte de cada tabela).
#
#   uso: scripts/loaded_at_migra.sh {sandbox|models|prod} [--dry-run]
#
#     sandbox  ingestion_sandbox   (perfil DB__DEV__*)
#     models   analytics_dev       (perfil DB__DEV__*, o banco que o dbt lê)
#     prod     produção            (perfil DB__PROD__*, exige escrita)
#
# Tudo é operação de catálogo (RENAME/ALTER DEFAULT): nenhuma linha é reescrita,
# as views do dbt sobre a raw sobrevivem (o Postgres reescreve a dependência) e
# o OID da tabela não muda. As linhas já gravadas ficam como estão — se o
# servidor não estiver em UTC, o histórico anterior à migração está no fuso dele.
#
# Ordem importa: os três bancos precisam ser migrados antes do próximo
# raw_copy.sh, que usa a lista de colunas da origem nos dois lados.
set -euo pipefail

SANDBOX_DB=ingestion_sandbox
MODELS_DB=analytics_dev
ENV_FILE="$(cd "$(dirname "$0")/.." && pwd)/.env"
DRY_RUN=0

usage() {
    echo "uso: $0 {sandbox|models|prod} [--dry-run]" >&2
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
        --dry-run) DRY_RUN=1 ;;
        *) usage ;;
    esac
    shift
done
[[ $# -eq 0 ]] || usage

case "$alvo" in
    sandbox | models)
        HOST=$(env_value DB__DEV__HOST) PORT=$(env_value DB__DEV__PORT)
        USER=$(env_value DB__DEV__USER) PASS=$(env_value DB__DEV__PASSWORD)
        [[ $alvo == sandbox ]] && DB=$SANDBOX_DB || DB=$MODELS_DB
        ;;
    prod)
        HOST=$(env_value DB__PROD__HOST) PORT=$(env_value DB__PROD__PORT)
        USER=$(env_value DB__PROD__USER) PASS=$(env_value DB__PROD__PASSWORD)
        DB=$(env_value DB__PROD__NAME)
        ;;
    *) usage ;;
esac

banco() { PGPASSWORD="$PASS" psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -v ON_ERROR_STOP=1 "$@"; }

fuso=$(banco -Atc "SELECT current_setting('TimeZone')")
echo "$DB@$HOST (TimeZone=$fuso)$([[ $DRY_RUN == 1 ]] && echo '  (dry-run)')"
[[ $fuso == UTC || $fuso == Etc/UTC ]] || echo "  ⚠️  servidor fora de UTC: as linhas já gravadas estão em $fuso"

# Toda coluna de carimbo dos schemas raw_*, com o DEFAULT atual, para decidir em
# bash o que cada tabela precisa (rename, DEFAULT, ou nada).
LISTA="
    SELECT n.nspname || '|' || c.relname || '|' || a.attname || '|' ||
           coalesce(pg_get_expr(d.adbin, d.adrelid), '')
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
    LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
    WHERE c.relkind = 'r' AND n.nspname LIKE 'raw\\_%'
      AND a.attname IN ('data_carga', 'loaded_at_utc', 'ingested_at')
    ORDER BY 1"

DEFAULT_OK="(now() AT TIME ZONE 'utc'::text)"
mexidas=0 ok=0

while IFS='|' read -r schema tabela coluna padrao; do
    [[ -n $schema ]] || continue
    # `ingested_at` (tabela de controle) mantém o nome: só o fuso do DEFAULT é
    # o mesmo das tabelas de dado.
    [[ $coluna == ingested_at ]] && final=ingested_at || final=loaded_at_utc
    acoes=()
    [[ $coluna == data_carga ]] && acoes+=("rename")
    [[ $padrao == "$DEFAULT_OK" ]] || acoes+=("default")
    if ((${#acoes[@]} == 0)); then
        ok=$((ok + 1))
        continue
    fi
    mexidas=$((mexidas + 1))
    echo "  $schema.$tabela.$final: ${acoes[*]}"
    [[ $DRY_RUN == 1 ]] && continue
    # Uma transação por tabela: o rename e o DEFAULT andam juntos.
    banco -q -c "
        BEGIN;
        SET LOCAL lock_timeout = '30s';
        $([[ $coluna == data_carga ]] && echo "ALTER TABLE $schema.$tabela RENAME COLUMN data_carga TO loaded_at_utc;")
        ALTER TABLE $schema.$tabela
            ALTER COLUMN $final SET DEFAULT (now() AT TIME ZONE 'utc');
        COMMIT;"
done <<<"$(banco -Atc "$LISTA")"

echo "$mexidas tabela(s) $([[ $DRY_RUN == 1 ]] && echo 'a migrar' || echo 'migrada(s)'), $ok já em dia"
