# Pipeline: Clima (OpenWeather)

Resumo meteorológico diário de um ponto fixo (Atibaia/SP) via API
[One Call 3.0 — day_summary](https://openweathermap.org/api/one-call-3#history_daily_aggregation).
Mesmo desenho do [`energia/solar`](../../energia/solar/README.md): high-water mark no
Postgres → extração só das datas faltantes → landing que acumula os JSONs → bronze
reconstruído a partir dele → full refresh da tabela.

Migrado do repo `openweather` (submódulo `include/openweather` do airflow3).

## Fluxo (`openweather_etl.py`, entidade `daily`)

1. **extract** — `core.missing_dates_from_db` lê `MAX(date)` de
   `raw_openweather.openweather_daily` (schema/tabela/coluna vêm do YAML), gera as
   datas faltantes até ontem (ou até hoje se já passou das 20h), grava
   `missing_dates.csv` e requisita o `day_summary` de cada data
   (`day_summary_YYYY-MM-DD.json`).
2. **transform** — `parse_day_summary` achata **cada JSON do landing**, converte
   Kelvin → Celsius e fixa tipos; `core.write_bronze` consolida em `all_dfs.csv`
   (`,`). É um rebuild completo: o landing é a fonte de verdade da tabela.
3. **load** — full refresh (`write: truncate`, o default): `TRUNCATE` + `COPY`
   do `all_dfs.csv` em `raw_openweather.openweather_daily`.

## Como executar

```bash
uv run python -m pipelines.clima.openweather.openweather_etl
uv run python -m pipelines.clima.openweather.openweather_etl --steps transform
```

Sem datas faltantes, o extract encerra com `Nenhuma data faltando.`

## Configuração

No `.env` da raiz: `OPENWEATHER_API_KEY=`. O banco é o do ambiente
(`DB__<ENV>__*`, `ingestion_sandbox` em dev); a tabela
`raw_openweather.openweather_daily` precisa existir lá para o high-water mark
(em dev: `scripts/raw_copy.sh seed raw_openweather`).
Latitude/longitude, arquivo de controle e coluna de data ficam em `options` no
YAML; `bronze_sep: ","` é só o formato histórico do CSV. Landing em
`${LAKE_ROOT}/raw/weather_project/` (acumula os JSONs e o `missing_dates.csv`),
bronze em `${LAKE_ROOT}/bronze/weather_project/`.

## Notas

- **A carga é da `core`**, não mais do DAG, e é full refresh: a tabela é função do
  landing. Os 1.824 JSONs reconstroem as 1.824 linhas em ~8 s. O que evita
  rebaixar o histórico é o extract, incremental por data.
- O transform gera `lat`/`lon`, que a tabela não tinha: entram como colunas novas
  (`ALTER TABLE ADD COLUMN`, com WARNING) na primeira carga. O staging do dbt
  seleciona coluna a coluna, então isso não o afeta.
- O índice único `openweather_date_pk` continua no banco e não atrapalha, mas
  deixou de ser necessário (era do upsert que o DAG fazia).
- **Um JSON do landing discorda do que foi carregado na época:**
  `day_summary_2025-03-25.json` traz `"date": "2025-3-25"` (sem zero à esquerda),
  e a tabela antiga tinha `2025-03-25`. O rebuild passa a refletir o arquivo, que
  é o certo; `'2025-3-25'::date` converte normalmente, então o staging do dbt não
  muda. É o tipo de divergência que o full refresh expõe e o upsert escondia.
- O token vai na query string; por isso a URL **não** é logada.
- `day_summary` exige o plano One Call 3.0 (1000 chamadas/dia grátis). Uma lacuna
  longa consome uma chamada por dia faltante.
- Placeholder `{day}` no nome do arquivo é resolvido aqui, não pela `core`
  (que só conhece `{date}` = hoje).

## Checklist para o DAG do Airflow (mudou nesta padronização)

- `missing_raw.identify_missing_dates(db)` deixou de existir. Equivalente:
  `core.missing_dates_from_db(PostgresClient(connection=hook.get_conn()), [sql], control)`,
  ou simplesmente rodar por etapa: `... openweather_etl --steps extract`
  e `--steps transform`.
- Módulo renomeado: `pipelines.clima.openweather.run` → `.openweather_etl`.
