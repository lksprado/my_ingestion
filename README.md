# my_ingestion

Monorepo de ingestão de dados pessoais: APIs públicas, scraping e planilhas →
data lake local (CSV/JSON) e Postgres (schema `raw`). Unifica os antigos repos
`demodados` (local_setup), `finance/investments`, `Solar`, `fundos-imobiliarios`,
`webscraping-inflation` e `webscraping-bookstore` num único ambiente.

## Estrutura

```
src/my_ingestion/
├── settings.py        # config central (pydantic-settings, lê o .env da raiz)
├── core/              # biblioteca compartilhada
│   ├── http.py        # HttpClient: requests com retry/backoff, fetch_and_save(_many)
│   ├── db.py          # PostgresClient: send_df/send_csv/load_files_to_table
│   ├── config.py      # load_yaml, PipelineConfig, load_source_config (${VAR} nos YAMLs)
│   ├── etl.py         # GenericETL (extract/transform/load plugáveis)
│   ├── io.py          # concat_files_to_df, write_csv, list_files
│   ├── text.py        # normalize_string, ColumnSanitizer
│   ├── logging.py     # setup_logger
│   └── parsers/       # json (normalize_json_object), html (make_bs_object)
└── pipelines/
    ├── legislativo/   # Câmara, Senado, e-Cidadania, Ranking Políticos, Radar Congresso
    ├── financas/      # investimentos (b3, avenue, google, fgc) e fundos_imobiliarios
    ├── precos/        # atacadao (inflação pessoal)
    ├── energia/       # solar (APsystems)
    └── livros/        # vide_editorial
```

Cada fonte tem sua pasta com código + `*_config.yml` (blocos `environments:
{local, airflow}` e `sources:`). Paths usam `${LAKE_ROOT}`/`${SEEDS_ROOT}` —
nada hardcoded, nada de credencial em código.

`demodados/demodadosdw/` é o projeto **dbt** (transformação) — repo git próprio,
fora deste monorepo (ignorado pelo `.gitignore`).

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
uv run python -m my_ingestion.pipelines.legislativo.camara.camara_deputados
uv run python -m my_ingestion.pipelines.financas.investimentos.run_all       # b3+avenue+google
uv run python -m my_ingestion.pipelines.financas.investimentos.run_all fgc
uv run python -m my_ingestion.pipelines.financas.fundos_imobiliarios.run --month 2026-09
uv run python -m my_ingestion.pipelines.precos.atacadao.run
uv run python -m my_ingestion.pipelines.energia.solar.run
uv run python -m my_ingestion.pipelines.livros.vide_editorial.run
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

## Notas da migração (2026-09)

- Convenções de env: `DB_PASSWORD` (não mais `DB_PW`), `APSYSTEMS_USER`/
  `APSYSTEMS_PASSWORD` (não mais `LOGIN`/`PW`), `GOOGLE_CREDENTIALS_FILE`
  (não mais `CREDENTIALS`).
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
