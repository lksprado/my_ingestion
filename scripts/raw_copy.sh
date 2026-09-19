#!/usr/bin/env bash
# Copia schemas raw_* entre os dois bancos de dev (mesmo servidor, perfil DB__DEV__* do .env).
#
#   scripts/raw_copy.sh seed    raw_x [raw_y ...]   analytics_dev     -> ingestion_sandbox
#   scripts/raw_copy.sh promote raw_x [raw_y ...]   ingestion_sandbox -> analytics_dev
#
# seed: antes de testar um pipeline incremental, para ele não baixar o histórico inteiro.
# promote: raw validada vai para o analytics_dev, onde o dbt a modela antes de existir em prod.
#
# Troca tabela a tabela: as da origem são recriadas no destino; as que só existem no
# destino ficam. O DROP é CASCADE porque o dbt tem views sobre a raw (ex.:
# staging_nhl.vw_stg_request_*); as views derrubadas aparecem nos NOTICEs e voltam
# no próximo dbt build.
set -euo pipefail

SANDBOX_DB=ingestion_sandbox
MODELS_DB=analytics_dev
ENV_FILE="$(cd "$(dirname "$0")/.." && pwd)/.env"

usage() {
    echo "uso: $0 {seed|promote} raw_<fonte> [raw_<fonte> ...]" >&2
    exit 1
}

[[ $# -ge 2 ]] || usage
case "$1" in
    seed) src=$MODELS_DB dst=$SANDBOX_DB ;;
    promote) src=$SANDBOX_DB dst=$MODELS_DB ;;
    *) usage ;;
esac
shift

env_value() {
    local line
    line=$(grep -E "^$1=" "$ENV_FILE" | tail -n1) || {
        echo "faltou $1 em $ENV_FILE" >&2
        exit 1
    }
    line=${line#*=}
    line=${line%\"}
    line=${line#\"}
    printf '%s' "$line"
}

export PGHOST PGPORT PGUSER PGPASSWORD
PGHOST=$(env_value DB__DEV__HOST)
PGPORT=$(env_value DB__DEV__PORT)
PGUSER=$(env_value DB__DEV__USER)
PGPASSWORD=$(env_value DB__DEV__PASSWORD)

schema_args=()
for schema in "$@"; do
    [[ $schema =~ ^raw_[a-z0-9_]+$ ]] || {
        echo "schema recusado: '$schema' (só raw_<fonte>)" >&2
        exit 1
    }
    exists=$(psql -d "$src" -Atc "SELECT 1 FROM pg_namespace WHERE nspname = '$schema'")
    [[ $exists == 1 ]] || {
        echo "schema $schema não existe em $src" >&2
        exit 1
    }
    schema_args+=(-n "$schema")
done

dump=$(mktemp)
list=$(mktemp)
trap 'rm -f "$dump" "$list"' EXIT

echo "$src -> $dst: $*"
pg_dump -d "$src" -Fc -f "$dump" "${schema_args[@]}"

# O schema é criado à parte: restaurá-lo tentaria recriar um que já existe.
pg_restore -l "$dump" | grep -v ' SCHEMA - ' >"$list"

{
    for schema in "$@"; do
        echo "CREATE SCHEMA IF NOT EXISTS $schema;"
    done
    pg_restore -l "$dump" |
        awk '$4 == "TABLE" && $5 != "DATA" { printf "DROP TABLE IF EXISTS %s.%s CASCADE;\n", $5, $6 }'
} | psql -d "$dst" -q -1 -v ON_ERROR_STOP=1

pg_restore -d "$dst" -L "$list" --no-owner --exit-on-error "$dump"

for schema in "$@"; do
    psql -d "$dst" -Atc "
        SELECT '$schema: ' || count(*) || ' tabela(s)'
        FROM pg_tables WHERE schemaname = '$schema'"
done
