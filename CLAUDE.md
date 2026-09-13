# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Monorepo de ingestão de dados pessoais: APIs públicas, scraping e planilhas → data lake local (CSV/JSON em `${LAKE_ROOT}`) e Postgres (schemas `raw_<fonte>` no banco do ambiente: `analytics_dev` em local). A transformação (dbt) vive **fora** deste repo (`~/workspace/demodados/demodadosdw` e `~/workspace/the_dw`). Código, comentários, docs e mensagens de commit são em **português**.

Documentação autoritativa (leia antes de mexer no que ela cobre):

- `src/core/README.md` — referência da biblioteca compartilhada (assinaturas, exemplos).
- `src/pipelines/README.md` — guia de design para **criar um pipeline novo** (onde colocar, YAML, esqueleto, checklist de pronto). Não duplique aqui; siga-o.
- Cada `src/pipelines/<domínio>/<fonte>/README.md` — tabelas, ordem de execução e armadilhas daquela fonte.

## Commands

```bash
uv sync                                  # Python 3.12 (pinado <3.13), instala src/ em modo editável
cp .env.example .env                     # obrigatório: settings.py falha na importação sem LAKE_ROOT/SEEDS_ROOT/DB__LOCAL__*
uv run pre-commit install                # ruff + gitleaks + check-yaml nos commits

uv run task test                         # pytest -v -m 'not integration'
uv run pytest tests/core/test_jsonb.py   # um arquivo
uv run pytest tests/core/test_jsonb.py::test_dict_with_array_key   # um teste
uv run pytest -m integration             # testes que exigem Postgres/serviços externos
uv run task lint                         # ruff check + ruff format --check
uv run task format                       # ruff format + ruff check --fix

uv run python -m pipelines.<domínio>.<fonte>.<script>   # roda um pipeline (ex.: pipelines.energia.solar.run)
uv run python -c "import pipelines.<domínio>.<fonte>.<script>"   # checa que o módulo importa isolado
```

## Layout plano e imports

`src/` é a raiz de código (hatchling `sources = ["src"]`): `core`, `pipelines` e `settings` ficam no topo, **sem prefixo de pacote**. Escreva `from core import HttpClient`, `from settings import settings`, `from pipelines.esportes.nhl._common import ...`. Nunca `from src.core ...` nem `from my_ingestion ...`. Não há ajuste de `sys.path` em `tests/conftest.py`; o `uv sync` resolve tudo.

## Arquitetura

**Três camadas.** `settings.py` (pydantic-settings, lê o `.env` da raiz ancorado em `REPO_ROOT`) → `core/` (biblioteca) → `pipelines/<domínio>/<fonte>/` (uma pasta por sistema de origem, um script por tabela destino).

**YAML descreve o quê, Python descreve como, `core` faz o resto.** Cada fonte tem um `<fonte>_config.yml` com blocos `environments: {local, airflow}` (paths base com `${LAKE_ROOT}`/`${SEEDS_ROOT}`) e `sources: {<entidade>: ...}`. `load_source_config(yml, source=...)` escolhe o ambiente por `settings.env` (var `ENV`) e devolve um dict para `PipelineConfig(**cfg)`. O mesmo código roda na máquina local e no Airflow por causa disso; não passe `env="local"` explicitamente em código novo.

**`PipelineConfig`** (`core/config.py`) normaliza `landing_dir`/`bronze_dir`/`parameter_dir` para `Path`, resolve `${VAR}` e `~`, cria os diretórios (`criar_dirs=True`; em testes use `criar_dirs=False`), e substitui **apenas** o placeholder `{date}` em `landing_file`/`bronze_file`. Outros placeholders (`{game_id}`, `{day}`) são preservados para o script resolver com `str.format`. Chaves que a `core` não interpreta vão no bloco `options:` do YAML e chegam em `cfg.options`.

**`GenericETL`** (`core/etl.py`) encadeia `extract → transform → load`. Defaults: extract baixa `cfg.url_base` para `cfg.landing_filepath` via `HttpClient`; **transform não tem default** (obrigatório); load lê o bronze com `sep=";"` e faz `PostgresClient().send_df_to_db(..., schema=cfg.db_schema, how="replace")` em `<db_schema>.<db_table>`; sem `db_schema` no YAML levanta `ValueError`. `load_fn=None` significa "use o loader padrão", não "não carregue". Bronze é sempre CSV com `;`.

**Encadeamento por parâmetros.** Um pipeline produtor declara `output_param_file` (`{arquivo: coluna}`) e chama `cfg.write_output_params(df)`; o consumidor declara `parameter_file`. Isso define a ordem de execução, documentada no README da fonte. Extração incremental por IDs compara três conjuntos: todos os IDs, os já baixados no `landing_dir`, e um CSV de "sem dados" no `parameter_dir` (para não martelar a API com IDs que nunca voltam).

**Pipelines que não usam `GenericETL`** (por design, não por dívida): `precos/atacadao` (saída é CSV para seed do dbt, sem banco), `financas/fundos_imobiliarios` (CLI mensal com `--month/--force`), `financas/investimentos` (`run_all.py` isola falhas por fonte), `esportes/nhl` (JSON bruto → tabela JSONB via `JsonbLoader`, IDs vêm de views `staging.vw_stg_request_*` do dbt, lógica em `_common.py`), `energia/solar` e `clima/openweather` (incremental por data com high-water mark via `core.incremental`). Critério: `GenericETL` quando o fluxo é landing → bronze → `raw_<fonte>.*`; fora disso, monte o seu mas continue usando `HttpClient`, `PostgresClient`, `core.io`, `setup_logger`.

**Um banco por ambiente, um schema por fonte.** `ENV` escolhe o perfil de conexão `DB__<ENV>__*` do `.env` (`settings.db_target`); `PostgresClient()` sem argumentos usa esse perfil. Em `ENV=local` o banco é obrigatoriamente `analytics_dev` (validator em `settings.py`); `ENV=airflow` é a produção em outro host. Não existe mais `DB_NAME`/`DW_DB_NAME`/`settings.dw_db` — nunca passe `db_name=` para desviar de banco. Toda escrita exige `schema=` explícito começando com `raw_` (`core.db.validate_raw_schema`; `send_df_to_db`, `send_csv_to_db`, `load_files_to_table`, `JsonbLoader` não têm default). O schema de uma fonte é declarado uma vez, na chave `db_schema` do topo do `<fonte>_config.yml`, e chega em `cfg.db_schema`. Leituras (`staging.*`, `intermediate.*`) não são restringidas.

**`HttpClient` devolve `None` em erro** em vez de levantar exceção. Trate o item, logue e siga; um ID quebrado não pode derrubar uma extração longa.

**Nomes de tabela preservados por compatibilidade com o dbt:** `raw_nhl.nhl_raw_*`, `raw_nhl.nhl_ingestion_control`, `raw_openweather.openweather_daily`, `raw_solar.solar_daily_energy|solar_hourly_energy` (só o schema seguiu o padrão `raw_<fonte>`), e as pastas do lake `raw/nhl/*`, `staging/weather_project`. Não renomeie.

## Convenções que importam ao editar

- Credencial nova: `.env` **e** `.env.example` (valor vazio) **e** campo em `settings.py`. Conexão de banco só via perfil `DB__<ENV>__*` (`DbTarget`). Nunca `os.getenv` espalhado. Campo obrigatório em `Settings` não leva default. Arquivo de credencial mora em `~/.secrets/`, o `.env` guarda só o caminho.
- Nunca caminho absoluto em YAML ou código; use `${LAKE_ROOT}`/`${SEEDS_ROOT}`. Exceção consciente e única: `legislativo/_params/dbt_seed_maker.py`.
- Nomes de coluna sempre via `ColumnSanitizer(df).sanitize_columns_names().df`.
- Cascata de nomes: pasta `<fonte>/`, script `<fonte>_<entidade>.py`, source YAML `<entidade>`, schema `raw_<fonte>` (chave `db_schema` no topo do YAML), tabela `<entidade>` (ex.: `raw_camara.deputados`). Deixe no fim do `__main__` o comentário com o comando `uv run python -m ...`.
- Logger em código novo: `setup_logger(__name__)` da `core`. Os pipelines do `legislativo` usam `logging.getLogger` + `basicConfig` por herança da migração; não copie.
- Testes em `tests/<domínio>/`, priorizando parsers. Marque com `@pytest.mark.integration` o que precisa de Postgres/rede.
- `ruff` com `E,F,I,N,W,UP,B`, linha 88. `E501` ignorado só em `pipelines/legislativo/**` e no `dividend_report.py`.
- Pipeline novo só está pronto com README na pasta da fonte (checklist completo na seção 7 de `src/pipelines/README.md`).

## Padrões de commit

- `feat:` Commits do tipo feat indicam que seu trecho de código está incluindo um novo recurso.

- `fix:` - Commits do tipo fix indicam que seu trecho de código commitado está solucionando um problema (bug fix).

- `doc:` - Commits do tipo docs indicam que houveram mudanças na documentação, como por exemplo no Readme do seu repositório. (Não inclui alterações em código).

- `test:` - Commits do tipo test são utilizados quando são realizadas alterações em testes, seja criando, alterando ou excluindo testes unitários. (Não inclui alterações em código)

- `build:` - Commits do tipo build são utilizados quando são realizadas modificações em arquivos de build e dependências.

- `refactor:` - Commits do tipo refactor referem-se a mudanças devido a refatorações que não alterem sua funcionalidade.

- `chore:` - Commits do tipo chore indicam atualizações de formatações de código, semicolons, trailing spaces, lint, como por exemplo adicionar um pacote no gitignore. (Não inclui alterações em código)

- `remove:` - Commits do tipo remove indicam a exclusão de arquivos, diretórios ou funcionalidades obsoletas ou não utilizadas, qualquer outra forma de limpeza do código-fonte, reduzindo o tamanho e a complexidade do projeto e mantendo-o mais organizado.
