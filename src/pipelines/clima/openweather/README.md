# Pipeline: Clima (OpenWeather)

Resumo meteorológico diário de um ponto fixo (Atibaia/SP) via API
[One Call 3.0 — day_summary](https://openweathermap.org/api/one-call-3#history_daily_aggregation).
Mesmo desenho do [`energia/solar`](../../energia/solar/README.md): high-water mark no
Postgres → extração só das datas faltantes → CSV consolidado no staging.

Migrado do repo `openweather` (submódulo `include/openweather` do airflow3).

## Fluxo (`openweather_daily.py`)

1. **extract** — `core.missing_dates_from_db` lê `MAX(date)` de
   `raw_openweather.openweather_daily` (schema/tabela/coluna vêm do YAML), gera as
   datas faltantes até ontem (ou até hoje se já passou das 20h), grava
   `missing_dates.csv` e requisita o `day_summary` de cada data
   (`day_summary_YYYY-MM-DD.json`).
2. **transform** — `_parsers.parse_day_summary` achata cada JSON, converte Kelvin →
   Celsius e fixa tipos; `core.write_bronze` consolida em `all_dfs.csv` (`,`).
3. **load** — `none`: o Airflow carrega (ver Notas).

## Como executar

```bash
uv run python -m pipelines.clima.openweather.openweather_daily
uv run python -m pipelines.clima.openweather.openweather_daily --steps transform
```

Sem datas faltantes, o extract encerra com `Nenhuma data faltando.`

## Configuração

No `.env` da raiz: `OPENWEATHER_API_KEY=`. O banco é o do ambiente
(`DB__<ENV>__*`, `analytics_dev` em local); a tabela
`raw_openweather.openweather_daily` precisa existir lá para o high-water mark.
Latitude/longitude, arquivo de controle e coluna de data ficam em `options` no
YAML; `bronze_sep: ","` porque o Airflow lê o CSV. Saídas em
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

## Checklist para o DAG do Airflow (mudou nesta padronização)

- `missing_raw.identify_missing_dates(db)` deixou de existir. Equivalente:
  `core.missing_dates_from_db(PostgresClient(connection=hook.get_conn()), [sql], control)`,
  ou simplesmente rodar o script por etapa: `... openweather_daily --steps extract`
  e `--steps transform`.
- Módulo renomeado: `pipelines.clima.openweather.run` → `.openweather_daily`.
