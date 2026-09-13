# Pipeline: Clima (OpenWeather)

Resumo meteorológico diário de um ponto fixo (Atibaia/SP) via API
[One Call 3.0 — day_summary](https://openweathermap.org/api/one-call-3#history_daily_aggregation).
Mesmo desenho do [`energia/solar`](../../energia/solar/README.md): high-water mark no
Postgres → extração só das datas faltantes → CSV consolidado no staging.

Migrado do repo `openweather` (submódulo `include/openweather` do airflow3).

## Fluxo

1. **Identificar lacunas** — `missing_raw.py` lê `MAX(date)` de
   `raw_openweather.openweather_daily` e gera as datas faltantes até ontem (ou até hoje se já
   passou das 20h), gravando `missing_dates.csv` no staging.
2. **Extrair** — `extraction.py` requisita o `day_summary` de cada data e grava
   `day_summary_YYYY-MM-DD.json`.
3. **Transformar** — `transforming.py` achata os JSONs, converte Kelvin → Celsius,
   fixa tipos e consolida em `all_dfs.csv`.

## Arquivos

| Arquivo | Papel |
|---|---|
| `openweather_config.yml` | Caminho do staging, URL, template do nome do arquivo, lat/lon |
| `run.py` | Entrypoint que costura as três etapas |
| `missing_raw.py` | High-water mark (`core.incremental`) — aceita psycopg2 ou `PostgresHook` |
| `extraction.py` | `get_day_summary(...)` via `HttpClient` |
| `transforming.py` | `parse_day_summary` (uma linha) e `parsing_daily_weather` (diretório) |

## Como executar

```bash
uv run python -m pipelines.clima.openweather.run
```

Sem datas faltantes, encerra com `No missing dates to process.`

## Configuração

No `.env` da raiz: `OPENWEATHER_API_KEY=` (era `MY_API` no repo antigo). O banco
é o do ambiente (`DB__<ENV>__*`, `analytics_dev` em local); a tabela
`raw_openweather.openweather_daily` precisa existir lá para o high-water mark.
Latitude/longitude ficam em `options` no YAML. Saídas em
`${LAKE_ROOT}/staging/weather_project/`.

## Notas

- **Não há etapa de load.** O orquestrador (Airflow, `dag_weather_etl`) carrega o
  CSV numa tabela staging, faz `INSERT ... ON CONFLICT (date)` em
  `raw_openweather.openweather_daily` e move os JSONs para `bronze/weather_project`.
- O token vai na query string; por isso a URL **não** é logada.
- `day_summary` exige o plano One Call 3.0 (1000 chamadas/dia grátis). Uma lacuna
  longa consome uma chamada por dia faltante.
- Placeholder `{day}` no nome do arquivo é resolvido aqui, não pela `core`
  (que só conhece `{date}` = hoje).
