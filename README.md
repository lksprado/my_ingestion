# my_ingestion

Monorepo de ingestão de dados pessoais: APIs públicas, scraping e planilhas →
data lake local (CSV/JSON) e Postgres (schemas `raw_<fonte>` no banco do ambiente). Unifica os antigos repos
`demodados` (local_setup), `finance/investments`, `Solar`, `fundos-imobiliarios`,
`webscraping-inflation`, `webscraping-bookstore`, `nhl-extraction` e `openweather`
num único ambiente — todos os submódulos de código do `airflow3` vivem aqui.

## Estrutura

```
src/                   # raiz de código (layout plano: imports sem prefixo de pacote)
├── settings.py        # config central (pydantic-settings, lê o .env da raiz)
├── core/              # biblioteca compartilhada
│   ├── http.py        # HttpClient: requests com retry/backoff, fetch_and_save(_many)
│   ├── db.py          # PostgresClient: send_df/send_csv/load_files_to_table
│   ├── config.py      # load_yaml, PipelineConfig, load_source_config (${VAR} nos YAMLs)
│   ├── etl.py         # GenericETL (extract/transform/load plugáveis)
│   ├── io.py          # concat_files_to_df, write_csv, list_files
│   ├── text.py        # normalize_string, ColumnSanitizer
│   ├── logging.py     # setup_logger
│   ├── jsonb.py       # JsonbLoader: JSON -> tabela JSONB via COPY, com controle de ingestão
│   ├── incremental.py # high-water mark por data (missing_dates, get_max_date)
│   └── parsers/       # json (normalize_json_object), html (make_bs_object)
└── pipelines/
    ├── legislativo/   # Câmara, Senado, e-Cidadania, Ranking Políticos, Radar Congresso
    ├── financas/      # investimentos (b3, avenue, google, fgc) e fundos_imobiliarios
    ├── precos/        # atacadao (inflação pessoal)
    ├── energia/       # solar (APsystems)
    ├── clima/         # openweather (day_summary, Atibaia)
    ├── esportes/      # nhl (9 endpoints -> JSONB)
    └── livros/        # vide_editorial
```

Cada fonte tem sua pasta com código + `*_config.yml` (blocos `environments:
{local, airflow}` e `sources:`) + um `README.md` próprio. Paths usam
`${LAKE_ROOT}`/`${SEEDS_ROOT}` — nada hardcoded, nada de credencial em código.

📖 Documentação:

- **[`src/core/README.md`](src/core/README.md)** — referência da biblioteca
  compartilhada: o que cada módulo oferece, assinaturas e exemplos de uso.
- **[`src/pipelines/README.md`](src/pipelines/README.md)** — guia de design para
  **implementar um pipeline novo**: onde colocar, o YAML, o esqueleto do script,
  os padrões a reaproveitar e quando fugir do `GenericETL`.

Cada pasta de fonte tem ainda seu próprio README com tabelas, dependências e
armadilhas específicas.

A transformação (dbt) vive **fora** deste monorepo, em repositórios próprios:
`~/workspace/demodados/demodadosdw` (dados legislativos) e `~/workspace/the_dw`
(finanças e inflação, alvo do `SEEDS_ROOT`).

## Setup

```bash
uv sync                          # cria .venv único (Python 3.12) e instala o pacote
cp .env.example .env             # preencher credenciais
uv run pre-commit install        # ruff + gitleaks nos commits
```

## Comandos

```bash
uv run task test                 # pytest (exceto marker integration)
uv run task lint                 # ruff check + format --check
uv run task format               # ruff format + fix

# Pipelines (exemplos)
uv run python -m pipelines.legislativo.camara.camara_deputados
uv run python -m pipelines.financas.investimentos.run_all       # b3+avenue+google
uv run python -m pipelines.financas.fundos_imobiliarios.run --month 2026-09
uv run python -m pipelines.precos.atacadao.run
uv run python -m pipelines.energia.solar.run
uv run python -m pipelines.clima.openweather.run
uv run python -m pipelines.esportes.nhl.nhl_games_summary       # depois: dbt + run_all
uv run python -m pipelines.esportes.nhl.run_all
uv run python -m pipelines.livros.vide_editorial.run
```

## ⚠️ Segredos vazados nos repos antigos — ROTACIONAR

Os repositórios originais no GitHub têm segredos no histórico. Este monorepo
nasceu com histórico limpo, mas **os valores antigos continuam expostos** e
precisam ser trocados nos serviços:

1. **Service account do Google** (`finances-py-a58b0d2a733f.json`, repo
   `finance`): revogar/recriar a chave no GCP. A chave atual foi movida para
   `~/.secrets/` (fora do repo) e é referenciada via `GOOGLE_CREDENTIALS_FILE`.
2. **Senha do Postgres** (repos `finance` e `Solar`, hardcoded no código).
3. **Login do portal APsystems** (repo `Solar`, `.env` commitado).
4. **JWT da Pluggy** (`meupluggy.py`, repo `finance`).
5. **Credenciais nos `.env`** dos repos `demodados` e `webscraping-bookstore`.
6. **Senha do Postgres** também hardcoded em `local_run/pipeline/extracting.py`
   do repo `nhl-extraction` (e `.env` commitado antes do `cc23dcb`).
7. **Chave da OpenWeather**: conferir o histórico do repo `openweather` (o `.env`
   está no `.gitignore`, mas houve commit "Remove generated files").

## Notas da migração (2026-09)

- Convenções de env: `DB_PASSWORD` (não mais `DB_PW`), `APSYSTEMS_USER`/
  `APSYSTEMS_PASSWORD` (não mais `LOGIN`/`PW`), `GOOGLE_CREDENTIALS_FILE`
  (não mais `CREDENTIALS`), `OPENWEATHER_API_KEY` (não mais `MY_API`).
- **Destino no Postgres (2026-09-13)**: um banco por ambiente e um schema por
  fonte. `ENV` escolhe o perfil `DB__<ENV>__*` do `.env` (`DB__LOCAL__HOST`,
  `DB__LOCAL__NAME=analytics_dev`...); em `local` o banco tem que ser
  `analytics_dev` (validado na importação), `airflow` é a produção em outro host.
  Toda carga vai para `raw_<fonte>.<entidade>` (`raw_camara.deputados`,
  `raw_b3.acoes`). Os antigos `DB_NAME` (`demodados`) e `DW_DB_NAME` (`postgres`)
  deixaram de existir; os dados desses bancos **não migram sozinhos** — as cargas
  são full refresh e recriam as tabelas no `analytics_dev`. Os projetos dbt
  (`demodadosdw`, `my_datawarehouse`) precisam reapontar `profiles.yml` e os
  `_sources.yml`.
- **2026-09-13**: chegaram `esportes/nhl` e `clima/openweather`, os dois últimos
  submódulos de código do `airflow3`. Nomes de **tabela** (`nhl_raw_*`,
  `openweather_daily`, `solar_*`) e pastas do lake (`raw/nhl/*`,
  `staging/weather_project`) foram preservados porque o dbt `my_datawarehouse` e os
  dados existentes dependem deles; só o schema virou `raw_nhl`/`raw_openweather`/
  `raw_solar`. O NHL lê os IDs a requisitar de views do dbt
  (`staging.vw_stg_request_*`), que precisam existir no banco do ambiente. As
  tabelas de solar e openweather são carregadas pelo Airflow, não por este repo.
- Também em 2026-09-13: o scraper do Atacadão ganhou a paginação (2 páginas) e o
  parsing defensivo que estavam só no working tree do submódulo `inflation`.
- Dados gerados saíram do repo. Entradas que viviam dentro dos repos antigos
  agora são esperadas no lake:
  - CSVs mensais da inflação → `${LAKE_ROOT}/bronze/inflation/months/`
  - `instituicoes_conglomerado_prudencial.csv` (fgc) →
    `${LAKE_ROOT}/raw/investments/instituicoes/`
  - dados de FII → `${LAKE_ROOT}/raw/fii/` (recuperáveis dos repos antigos)
- Projeto `ecidadania` standalone foi descartado (duplicado do pipeline
  legislativo/ecidadania, mais completo).
- Testes obsoletos descartados: `test_transform_ranking_politicos`
  (função inexistente), `test_extraction`/`test_transformation` do solar
  (classes antigas), `test_postgres` (conexão real com senha).
- Loaders agora acrescentam `arquivo_origem`/`data_carga` também nas cargas
  ex-investments/books (comportamento padrão do `PostgresClient`).
