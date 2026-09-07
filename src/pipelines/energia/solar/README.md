# Pipeline: Energia Solar

Extrai dados de produção de um sistema solar residencial (APsystems / portal
`apsystemsema.com`), que **não tem API pública** — os dados ficam atrás de login e
de uma view do app que habilita uma API interna. Daí o uso de Selenium para
autenticar e, a partir dos cookies da sessão, chamar essa API diretamente.

[Dashboard no Tableau](https://public.tableau.com/app/profile/lucas8230/viz/HOMESOLARPANELPRODUCTION2021-2024/Painel1)

## Fluxo

1. **Identificar lacunas** — `missing_raw.py` consulta o maior `date`/`datetime` já
   carregado em `raw.solar_daily_energy` e `raw.solar_hourly_energy` e gera a lista
   de datas faltantes até ontem (ou até hoje, se já passou das 20h).
2. **Extrair** — `extraction.py` faz login com Selenium, navega até o relatório e
   requisita o JSON de produção por data, salvando no staging.
3. **Transformar** — `transforming.py` converte os JSONs em DataFrames e produz os
   agregados horário e diário.

## Arquivos

| Arquivo | Papel |
|---|---|
| `run.py` | Entrypoint que costura as três etapas |
| `missing_raw.py` | High-water mark no Postgres + arquivo de controle de datas |
| `extraction.py` | Login Selenium, sessão autenticada e download dos JSONs |
| `transforming.py` | JSON → DataFrame, agregações diária e horária |

## Como executar

```bash
uv run python -m pipelines.energia.solar.run
```

Sem datas faltantes, o script encerra sem fazer nada (`No missing dates to process`).

## Configuração

No `.env` da raiz:

```
APSYSTEMS_USER=
APSYSTEMS_PASSWORD=
```

Saídas em `${LAKE_ROOT}/staging/solar_project/`, incluindo o arquivo de controle
`missing_dates.csv`.

## Notas

- **Não há etapa de load.** O Postgres é lido apenas para descobrir até onde os
  dados já vão; a carga fica a cargo do orquestrador (Airflow) no repositório
  privado que consome este pipeline. As saídas são CSVs diário e horário.
- O Selenium roda **com janela** por padrão (`headless=False` no `run.py`), porque o
  portal se comporta mal em headless. Para rodar sem display, mude o
  `SeleniumConfig` e valide que o login ainda passa.
- `missing_raw._get_first` aceita tanto uma conexão psycopg2 quanto um
  `PostgresHook` do Airflow — é o que permite reaproveitar o módulo nos dois
  contextos.
- Este pipeline foi migrado do repo `Solar`; os testes originais referenciavam
  classes que não existem mais (`EMAWebScraper`, `TransformCSV`) e foram
  descartados na migração, então hoje ele **não tem cobertura de testes**.
