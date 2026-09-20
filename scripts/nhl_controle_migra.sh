#!/usr/bin/env bash
# Corrige o table_schema legado em raw_nhl.nhl_ingestion_control (perfil DB__DEV__* do .env).
#
#   scripts/nhl_controle_migra.sh [banco ...]      # default: ingestion_sandbox analytics_dev
#
# Em prod, exporte PGHOST/PGPORT/PGUSER/PGPASSWORD antes (o perfil DB__DEV__* só
# é lido para as variáveis que você não definiu) e passe o banco:
#   PGHOST=... PGUSER=... PGPASSWORD=... scripts/nhl_controle_migra.sh analytics_prod
#
# O repo nhl-extraction registrava os ~168 mil arquivos já ingeridos com
# table_schema = 'nhl'; aqui o schema é 'raw_nhl'. Como o JsonbLoader filtra o
# controle por (table_schema, table_name), as linhas antigas ficam invisíveis: a
# carga reinseriria tudo duplicado e as consultas de parâmetro (params_* do
# nhl_etl.py) pediriam de novo todos os game_id. Este script renomeia o schema
# das linhas legadas. É idempotente: rodar de novo não faz nada.
set -euo pipefail

ENV_FILE="$(cd "$(dirname "$0")/.." && pwd)/.env"
if [[ $# -ge 1 ]]; then bancos=("$@"); else bancos=(ingestion_sandbox analytics_dev); fi

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
PGHOST=${PGHOST:-$(env_value DB__DEV__HOST)}
PGPORT=${PGPORT:-$(env_value DB__DEV__PORT)}
PGUSER=${PGUSER:-$(env_value DB__DEV__USER)}
PGPASSWORD=${PGPASSWORD:-$(env_value DB__DEV__PASSWORD)}

resumo() {
    psql -d "$1" -Atc "
        SELECT coalesce(string_agg(table_schema || '=' || n, ', ' ORDER BY table_schema), 'vazio')
        FROM (SELECT table_schema, count(*) AS n
              FROM raw_nhl.nhl_ingestion_control GROUP BY 1) AS t"
}

for banco in "${bancos[@]}"; do
    existe=$(psql -d "$banco" -Atc "
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'raw_nhl' AND tablename = 'nhl_ingestion_control'")
    if [[ $existe != 1 ]]; then
        echo "$banco: sem raw_nhl.nhl_ingestion_control, pulando"
        continue
    fi

    echo "$banco: antes  -> $(resumo "$banco")"

    # Linha legada que já tem equivalente em raw_nhl violaria a PK: sobe só quem
    # ainda não existe, e a sobra sai no comando seguinte.
    psql -d "$banco" -q -1 -v ON_ERROR_STOP=1 \
        -c "UPDATE raw_nhl.nhl_ingestion_control AS c
            SET table_schema = 'raw_nhl'
            WHERE c.table_schema = 'nhl'
              AND NOT EXISTS (
                  SELECT 1 FROM raw_nhl.nhl_ingestion_control AS j
                  WHERE j.table_schema = 'raw_nhl'
                    AND j.table_name = c.table_name
                    AND j.filename = c.filename
              )" \
        -c "DELETE FROM raw_nhl.nhl_ingestion_control WHERE table_schema = 'nhl'"

    echo "$banco: depois -> $(resumo "$banco")"
done
