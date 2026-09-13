# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Monorepo de ingestão de dados pessoais: APIs públicas, scraping e planilhas → data lake local (CSV/JSON em `${LAKE_ROOT}`) e Postgres (schemas `raw_<fonte>` no banco do ambiente: `analytics_dev` em local). A transformação (dbt) vive **fora** deste repo (`~/workspace/demodados/demodadosdw` e `~/workspace/the_dw`). Código, comentários, docs e mensagens de commit são em **português**.

Documentação autoritativa (leia antes de mexer no que ela cobre):

- `src/core/README.md` — referência da biblioteca compartilhada (contrato, assinaturas, exemplos).
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

uv run python -m pipelines.<domínio>.<fonte>.<fonte>_<entidade>                          # roda um pipeline inteiro
uv run python -m pipelines.<domínio>.<fonte>.<fonte>_<entidade> --steps transform,load   # só algumas etapas
uv run python -c "import pipelines.<domínio>.<fonte>.<script>"   # checa que o módulo importa isolado
```

## Layout plano e imports

`src/` é a raiz de código (hatchling `sources = ["src"]`): `core`, `pipelines` e `settings` ficam no topo, **sem prefixo de pacote**. Escreva `from core import HttpClient`, `from settings import settings`, `from pipelines.esportes.nhl._common import ...`. Nunca `from src.core ...` nem `from my_ingestion ...`. Não há ajuste de `sys.path` em `tests/conftest.py`; o `uv sync` resolve tudo.

## Arquitetura

**Três camadas.** `settings.py` (pydantic-settings, lê o `.env` da raiz ancorado em `REPO_ROOT`) → `core/` (biblioteca) → `pipelines/<domínio>/<fonte>/` (uma pasta por sistema de origem, um script por tabela destino, `_common.py` para o que a fonte ou o domínio compartilham).

**Um contrato: extract → transform → load, cada etapa lendo e escrevendo disco.** `extract(cfg)` grava arquivos brutos em `cfg.landing_dir`; `transform(cfg)` produz o bronze **sempre via `write_bronze(cfg, df)`** (ou `write_bronze_streaming` para milhares de arquivos): sanitiza colunas, remove CR/LF, grava `cfg.bronze_filepath` com `cfg.bronze_sep` (`;`); `load` vai para `raw_<fonte>.<tabela>`. As etapas são tasks separadas no Airflow, então nada passa em memória entre elas.

**YAML descreve o quê, Python descreve como, `core` faz o resto.** Cada fonte tem um `<fonte>_config.yml` com `db_schema`, opcionalmente `load`/`bronze_sep`/`options` no topo (default do arquivo, sobrescrevível por source), blocos `environments: {local, airflow}` (paths com `${LAKE_ROOT}`/`${SEEDS_ROOT}`) e `sources: {<entidade>: ...}`. `PipelineConfig.from_yaml(yml, source)` escolhe o ambiente por `settings.env` (var `ENV`); não passe `env=` em código de pipeline. `PipelineConfig` normaliza os diretórios para `Path`, resolve `${VAR}`, cria os diretórios (`criar_dirs=True`; em testes `False`) e substitui **apenas** `{date}` em `landing_file`/`bronze_file`; outros placeholders (`{id}`, `{day}`, `{game_id}`) ficam para o script resolver com `str.format`. Chaves que a `core` não interpreta vão em `options:` e chegam em `cfg.options`.

**`GenericETL`** (`core/etl.py`): `GenericETL(cfg, extract_fn=..., transform_fn=..., load_fn=None, log=logger)` + `run(steps=...)`. Defaults: extract baixa `cfg.url_base` para `cfg.landing_filepath` (sem `url_base`, no-op: landing alimentado por fora); transform é no-op (fontes JSON → JSONB); load segue `cfg.load` — `table` (bronze CSV em chunks → `send_df_to_db`, `replace`), `files` (cada CSV do bronze_dir → tabela de mesmo nome), `jsonb` (JSONs do landing → `JsonbLoader`, com `options.array_key/overwrite/control_table/file_pattern`), `none` (orquestrador carrega). Sem `db_schema` `raw_*` levanta `ValueError`. Todo script expõe `build() -> GenericETL` e termina em `run_cli(build)`, que dá `--steps` e configura o log.

**Encadeamento por parâmetros.** Produtor declara `output_param_file` (`{arquivo: coluna}`) e chama `cfg.write_output_params(df, default_column=...)`; consumidor declara `parameter_file`. Isso define a ordem de execução, documentada no README da fonte. Incremental por ID: `core.pending_ids`/`mark_no_data` (todos os IDs − já no landing − CSV "sem dados"); no legislativo, `_common.extract_by_ids(cfg)` faz o loop a partir de `{id}` em `base_url`/`landing_file` e `options.no_data_file`. Incremental por data: `core.missing_dates_from_db`.

**Exceções ao `GenericETL`** (não escrevem em `raw_*`): `precos/atacadao` (CSV → seed do dbt), `financas/fundos_imobiliarios` (CLI mensal + relatório), `financas/investimentos/investimentos_fgc.py` (DW → seed). Mesmo aí, use `HttpClient`, `PostgresClient.read_sql`, `write_csv`, `concat_files_to_df`, `setup_logger()`. Tudo o mais está no padrão, inclusive NHL (`load: jsonb`) e solar/openweather (`load: none`, Airflow carrega).

**Um banco por ambiente, um schema por fonte.** `ENV` escolhe o perfil de conexão `DB__<ENV>__*` do `.env` (`settings.db_target`); `PostgresClient()` sem argumentos usa esse perfil; no Airflow, `PostgresClient(connection=hook.get_conn())`. Em `ENV=local` o banco é obrigatoriamente `analytics_dev` (validator em `settings.py`); `ENV=airflow` é a produção em outro host. Nunca passe `db_name=` para desviar de banco. Toda escrita exige `schema=` explícito começando com `raw_` (`core.db.validate_raw_schema`; `send_df_to_db`, `load_files_to_table`, `JsonbLoader` não têm default). O schema de uma fonte é declarado uma vez, na chave `db_schema` do topo do `<fonte>_config.yml`. Leituras (`staging.*`, `intermediate.*`) via `read_sql` não são restringidas.

**`HttpClient` devolve `None` em erro** em vez de levantar exceção (`get_json`, `get_text`, `request`, `fetch_and_save*`). Trate o item, logue e siga; um ID quebrado não pode derrubar uma extração longa.

**Nomes de tabela e pastas do lake preservados por compatibilidade com o dbt/Airflow:** `raw_nhl.nhl_raw_*`, `raw_nhl.nhl_ingestion_control`, `raw_openweather.openweather_daily`, `raw_solar.solar_daily_energy|solar_hourly_energy`, tabelas `legislaturas`/`proposicao`/`processo`/`bignumbers`, e as pastas `raw/nhl/*`, `staging/weather_project`, `staging/solar_project`, `raw/demodados/*`, `raw/vide`. CSVs de solar/openweather usam `bronze_sep: ","` porque o Airflow os lê. Não renomeie.

## Convenções que importam ao editar

- Credencial nova: `.env` **e** `.env.example` (valor vazio) **e** campo em `settings.py`. Conexão de banco só via perfil `DB__<ENV>__*` (`DbTarget`). Nunca `os.getenv` espalhado. Campo obrigatório em `Settings` não leva default. Arquivo de credencial mora em `~/.secrets/`, o `.env` guarda só o caminho. Grupos de valores usam delimitador duplo (`URL_FINANCE__<CHAVE>` → `settings.url_finance`).
- Nunca caminho absoluto em YAML ou código; use `${LAKE_ROOT}`/`${SEEDS_ROOT}`. Exceção consciente e única: `legislativo/_params/dbt_seed_maker.py`.
- Bronze só via `write_bronze`/`write_bronze_streaming`; nomes de coluna vêm de `sanitize_columns` (aplicado ali). `sanitize_values(df, exclude=[...])` quando precisar normalizar valores preservando URLs/e-mails. `ColumnSanitizer` não existe mais.
- Cascata de nomes: pasta `<fonte>/`, script `<fonte>_<entidade>.py`, source YAML `<entidade>`, schema `raw_<fonte>` (chave `db_schema` no topo do YAML), tabela `<entidade>`. Deixe no fim do `__main__` o comentário com o comando `uv run python -m ... [--steps ...]`.
- Logger: módulo usa `logging.getLogger(__name__)`; só o entrypoint chama `setup_logger()` (o `run_cli`/`run_many` já fazem). Nunca `logging.basicConfig`.
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
