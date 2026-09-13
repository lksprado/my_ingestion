# Pipeline: Energia Solar

Extrai dados de produção de um sistema solar residencial (APsystems / portal
`apsystemsema.com`), que **não tem API pública** — os dados ficam atrás de login e
de uma view do app que habilita uma API interna. Daí o uso de Selenium para
autenticar e, a partir dos cookies da sessão, chamar essa API diretamente.
Configuração em `solar_config.yml`.

[Dashboard no Tableau](https://public.tableau.com/app/profile/lucas8230/viz/HOMESOLARPANELPRODUCTION2021-2024/Painel1)

## O que gera

| Script | Source | Etapas | Saída (staging) |
|---|---|---|---|
| `solar_daily_energy.py` | `daily_energy` | extract + transform | `daily_energy.csv` |
| `solar_hourly_energy.py` | `hourly_energy` | transform (lê o mesmo landing) | `hourly_energy.csv` |

Rode o **daily primeiro**: é ele que faz a extração (um JSON por dia,
`hourly24_production_YYYY-MM-DD.json`). Sem load (`load: none`): o Airflow carrega em
`raw_solar.solar_daily_energy` / `solar_hourly_energy`.

## Fluxo

1. **extract** (`_common.extract`) — `core.missing_dates_from_db` consulta o maior
   `date`/`datetime` das duas tabelas (`options.watermarks`), gera as datas
   faltantes até ontem (ou hoje, após as 20h) e grava `missing_dates.csv`; depois
   faz login com Selenium (`_extraction.py`), navega até o relatório e requisita o
   JSON de cada data com o `HttpClient` usando os cookies da sessão.
2. **transform** (`_parsers.py`) — concatena os JSONs (data extraída do nome do
   arquivo) e produz o agregado diário ou o horário; `core.write_bronze` grava o CSV
   com `,`.

## Como executar

```bash
uv run python -m pipelines.energia.solar.solar_daily_energy
uv run python -m pipelines.energia.solar.solar_hourly_energy
uv run python -m pipelines.energia.solar.solar_daily_energy --steps transform   # sem Selenium
```

Sem datas faltantes, o extract encerra com `Nenhuma data faltando.`

## Configuração

No `.env` da raiz: `APSYSTEMS_USER=` e `APSYSTEMS_PASSWORD=`; banco pelo perfil
`DB__<ENV>__*` (`raw_solar.*` precisa existir lá). ID do equipamento, URLs do
portal, `headless` e arquivo de controle ficam em `options` no YAML. Saídas em
`${LAKE_ROOT}/staging/solar_project/`.

## Notas

- **Não há etapa de load.** O Postgres é lido apenas para descobrir até onde os
  dados já vão; a carga fica a cargo do Airflow.
- O Selenium roda **com janela** por padrão (`headless: false` no YAML), porque o
  portal se comporta mal em headless.
- `all_dfs.csv` (consolidado intermediário) **não é mais gravado**; confirme que o
  DAG não o lê.
- Testes dos parsers em `tests/energia/test_solar_parsers.py`.

## Checklist para o DAG do Airflow (mudou nesta padronização)

- `missing_raw.identify_missing_dates(db)` deixou de existir. Equivalente:
  `core.missing_dates_from_db(PostgresClient(connection=hook.get_conn()), sqls, control)`,
  ou rodar os scripts por etapa com `--steps`.
- Módulo renomeado: `pipelines.energia.solar.run` → `.solar_daily_energy` +
  `.solar_hourly_energy`.
