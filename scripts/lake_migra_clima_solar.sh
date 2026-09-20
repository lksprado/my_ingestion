#!/usr/bin/env bash
# Move o lake de clima/solar para o layout padrão das demais fontes.
#
#   scripts/lake_migra_clima_solar.sh --dry-run   # mostra o que faria
#   scripts/lake_migra_clima_solar.sh             # executa
#
# Antes (o Airflow movia os JSONs para bronze/ depois de carregar):
#   staging/<projeto>/   base_raw E base_bronze — só o missing_dates.csv
#   bronze/<projeto>/    os JSONs de todo o histórico
#
# Depois (landing acumula, bronze é derivado — como raw/demodados, raw/nhl):
#   raw/<projeto>/       landing: os JSONs + missing_dates.csv
#   bronze/<projeto>/    bronze: os CSVs do transform
#
# Idempotente: rodar de novo não faz nada. Roda nos dois lakes (dev pelo .env,
# prod em /usr/local/airflow/mylake, com LAKE_ROOT= na frente do comando).
set -euo pipefail

PROJETOS=(weather_project solar_project)
DRY_RUN=0
[[ ${1:-} == "--dry-run" ]] && DRY_RUN=1

if [[ -z ${LAKE_ROOT:-} ]]; then
    ENV_FILE="$(cd "$(dirname "$0")/.." && pwd)/.env"
    linha=$(grep -E '^LAKE_ROOT=' "$ENV_FILE" | tail -n1) || {
        echo "faltou LAKE_ROOT em $ENV_FILE (ou exporte LAKE_ROOT)" >&2
        exit 1
    }
    LAKE_ROOT=${linha#*=}
    LAKE_ROOT=${LAKE_ROOT%\"}
    LAKE_ROOT=${LAKE_ROOT#\"}
fi
[[ -d $LAKE_ROOT ]] || {
    echo "LAKE_ROOT não existe: $LAKE_ROOT" >&2
    exit 1
}
echo "lake: $LAKE_ROOT$([[ $DRY_RUN == 1 ]] && echo '  (dry-run)')"

executa() {
    if [[ $DRY_RUN == 1 ]]; then
        echo "   + $*"
    else
        "$@"
    fi
}

# Move um arquivo copiando e só então apagando a origem.
#
# Não dá para usar `mv`: no SeaweedFS cada pasta de topo de /buckets é um bucket
# próprio, e rename entre buckets devolve EIO -- o mv nem cai no fallback de
# copiar, porque o erro não é EXDEV. Copiar e conferir o tamanho antes de apagar
# também deixa a migração retomável: arquivo já no destino só some da origem.
mover_arquivo() {
    local arquivo=$1 destino=$2
    local nome=${arquivo##*/}
    if [[ -e $destino/$nome ]] && [[ $(stat -c%s "$arquivo") == $(stat -c%s "$destino/$nome") ]]; then
        rm -f "$arquivo"
        return 0
    fi
    # -p pode falhar em FUSE ao preservar metadados; o dado importa mais.
    cp -p "$arquivo" "$destino/$nome" 2>/dev/null || cp "$arquivo" "$destino/$nome" || return 1
    [[ $(stat -c%s "$arquivo") == $(stat -c%s "$destino/$nome") ]] || return 1
    rm -f "$arquivo"
}

mover_lote() {
    local destino=$1
    shift
    local feitos=0 falhas=0
    for arquivo in "$@"; do
        if mover_arquivo "$arquivo" "$destino"; then
            feitos=$((feitos + 1))
        else
            falhas=$((falhas + 1))
            echo "   ❌ falhou: $arquivo" >&2
        fi
    done
    echo "   $feitos movido(s)$([[ $falhas -gt 0 ]] && echo ", $falhas com falha")"
    [[ $falhas == 0 ]]
}

for projeto in "${PROJETOS[@]}"; do
    landing="$LAKE_ROOT/raw/$projeto"
    bronze="$LAKE_ROOT/bronze/$projeto"
    staging="$LAKE_ROOT/staging/$projeto"
    echo "== $projeto"
    executa mkdir -p "$landing" "$bronze"

    # JSONs: bronze (onde o Airflow os despejava) -> landing
    if [[ -d $bronze ]]; then
        mapfile -t jsons < <(find "$bronze" -maxdepth 1 -name '*.json' -print)
        if ((${#jsons[@]})); then
            echo "   ${#jsons[@]} JSON(s) bronze/ -> raw/"
            if [[ $DRY_RUN == 1 ]]; then
                echo "   + copia ${#jsons[@]} arquivo(s) para $landing/ e apaga a origem"
            else
                mover_lote "$landing" "${jsons[@]}"
            fi
        else
            echo "   nenhum JSON em bronze/ (já migrado?)"
        fi
    fi

    # controle de datas: staging -> landing (é entrada do extract, não bronze)
    if [[ -f $staging/missing_dates.csv ]]; then
        echo "   missing_dates.csv staging/ -> raw/"
        if [[ $DRY_RUN == 1 ]]; then
            echo "   + copia $staging/missing_dates.csv para $landing/ e apaga a origem"
        else
            mover_arquivo "$staging/missing_dates.csv" "$landing"
        fi
    fi

    # CSVs soltos no staging eram o bronze antigo; o transform regrava em bronze/
    if [[ -d $staging ]]; then
        mapfile -t csvs < <(
            find "$staging" -maxdepth 1 -name '*.csv' ! -name 'missing_dates.csv' -print
        )
        ((${#csvs[@]})) && echo "   ${#csvs[@]} CSV(s) antigo(s) em staging/ serão removidos"
        for csv in "${csvs[@]:-}"; do [[ -n $csv ]] && executa rm -f "$csv"; done
        if [[ $DRY_RUN == 0 && -d $staging ]]; then
            rmdir "$staging" 2>/dev/null && echo "   staging/$projeto removido (vazio)"
        fi
    fi

    if [[ $DRY_RUN == 0 ]]; then
        echo "   landing: $(find "$landing" -maxdepth 1 -name '*.json' | wc -l) JSON(s)"
        restantes=$(find "$bronze" -maxdepth 1 -name '*.json' | wc -l)
        ((restantes)) && echo "   ⚠️  $restantes JSON(s) ainda em bronze/ (nome repetido?)"
    fi
done
echo "pronto."
