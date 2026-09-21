# Pipeline: Energia Solar

Extrai dados de produção de um sistema solar residencial (APsystems / portal
`apsystemsema.com`), que **não tem API pública** — os dados ficam atrás de login e
de uma view do app que habilita uma API interna. Daí o uso de Selenium para
autenticar e, a partir dos cookies da sessão, chamar essa API diretamente.
ETL em `solar_etl.py` (Selenium e parsers inclusos); configuração em `solar_config.yml`.

[Dashboard no Tableau](https://public.tableau.com/app/profile/lucas8230/viz/HOMESOLARPANELPRODUCTION2021-2024/Painel1)

## O que gera

| Entidade | Etapas | Bronze | Tabela |
|---|---|---|---|
| `daily_energy` | extract + transform + load | `daily_energy.csv` | `raw_apsystem.solar_daily_energy` |
| `hourly_energy` | transform + load (lê o mesmo landing) | `hourly_energy.csv` | `raw_apsystem.solar_hourly_energy` |

O **daily vem primeiro** (ordem de `ETLS`): é ele que faz a extração (um JSON por dia,
`hourly24_production_YYYY-MM-DD.json`). A carga é full refresh (`write: truncate`,
o default): cada tabela é função do landing.

## Fluxo

1. **extract** (`extract`) — `core.missing_dates_from_db` consulta o maior
   `date`/`datetime` das duas tabelas (`options.watermarks`), gera as datas
   faltantes até ontem (ou hoje, após as 20h) e grava `missing_dates.csv`; depois
   faz login com Selenium (`setup_driver`/`login`/`navigate_to_report`), navega até o relatório e requisita o
   JSON de cada data com o `HttpClient` usando os cookies da sessão.
2. **transform** (`load_landing` + `daily_summary`/`hourly`) — concatena os JSONs (data extraída do nome do
   arquivo) e produz o agregado diário ou o horário; `core.write_bronze` grava o CSV
   com `,`.

## Como executar

```bash
uv run python -m pipelines.energia.solar.solar_etl                        # daily + hourly
uv run python -m pipelines.energia.solar.solar_etl --steps transform      # sem Selenium
uv run python -m pipelines.energia.solar.solar_etl hourly_energy --steps transform
```

Sem datas faltantes, o extract encerra com `Nenhuma data faltando.`

## Configuração

No `.env` da raiz: `APSYSTEMS_USER=` e `APSYSTEMS_PASSWORD=`; banco pelo perfil
`DB__<ENV>__*` (`raw_apsystem.*` precisa existir lá). ID do equipamento, URLs do
portal, `headless` e arquivo de controle ficam em `options` no YAML. Landing em
`${LAKE_ROOT}/raw/solar_project/` (acumula os JSONs e o `missing_dates.csv`),
bronze em `${LAKE_ROOT}/bronze/solar_project/`.

## Notas

- **A carga é full refresh a partir do landing**, e o que evita rebaixar o
  histórico é o extract (high-water mark nas próprias tabelas). O landing é a
  fonte de verdade: os 1.821 JSONs reconstroem as 1.819 linhas de
  `solar_daily_energy` e as 43.656 de `solar_hourly_energy` em ~16 s.
- Os índices únicos `solar_daily_energy_date_pk` e
  `solar_hourly_energy_datetime_pk` existem no banco e não atrapalham; a carga
  não depende deles.
- O Selenium roda **com janela** por padrão (`headless: false` no YAML), porque o
  portal se comporta mal em headless.
- Sobrou um `all_dfs.csv` em `bronze/solar_project/` que nenhuma etapa lê nem
  regrava; pode ser apagado à mão.
- Testes dos parsers em `tests/energia/test_solar_parsers.py`.
