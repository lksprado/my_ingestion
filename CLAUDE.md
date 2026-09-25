# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Monorepo de ingestão de dados pessoais: APIs públicas, scraping e planilhas → data lake local (CSV/JSON em `${LAKE_ROOT}`) e Postgres (schemas `raw_<fonte>` no banco do ambiente: `ingestion_sandbox` em dev). A transformação (dbt) vive **fora** deste repo (`~/workspace/demodados/demodadosdw` e `~/workspace/my_analytics`). Código, comentários, docs e mensagens de commit são em **português**.

Documentação autoritativa (leia antes de mexer no que ela cobre):

- `src/core/README.md` — referência da biblioteca compartilhada (contrato, assinaturas, exemplos).
- `src/pipelines/README.md` — guia de design para **criar um pipeline novo** (onde colocar, YAML, esqueleto, checklist de pronto). Não duplique aqui; siga-o.
- Cada `src/pipelines/<domínio>/<fonte>/README.md` — tabelas, ordem de execução e armadilhas daquela fonte.

## Commands

```bash
uv sync                                  # Python 3.12 (pinado <3.13), instala src/ em modo editável
cp .env.example .env                     # obrigatório: settings.py falha na importação sem LAKE_ROOT/SEEDS_ROOT/DB__DEV__*
uv run pre-commit install                # ruff + gitleaks + check-yaml nos commits

uv run task test                         # pytest -v -m 'not integration'
uv run pytest tests/core/test_jsonb.py   # um arquivo
uv run pytest tests/core/test_jsonb.py::test_dict_with_array_key   # um teste
uv run pytest -m integration             # testes que exigem Postgres/serviços externos
uv run task lint                         # ruff check + ruff format --check
uv run task format                       # ruff format + ruff check --fix

uv run python -m pipelines.<domínio>.<fonte>.<fonte>_etl                                   # todas as entidades da fonte, na ordem
uv run python -m pipelines.<domínio>.<fonte>.<fonte>_etl <entidade> [...]                  # só essas entidades
uv run python -m pipelines.<domínio>.<fonte>.<fonte>_etl <entidade> --steps transform,load # só algumas etapas
uv run python -c "import pipelines.<domínio>.<fonte>.<fonte>_etl"   # checa que o módulo importa isolado
```

## Layout plano e imports

`src/` é a raiz de código (hatchling `sources = ["src"]`): `core`, `pipelines` e `settings` ficam no topo, **sem prefixo de pacote**. Escreva `from core import HttpClient`, `from settings import settings`, `from pipelines.esportes.nhl.nhl_etl import ...`. Nunca `from src.core ...` nem `from my_ingestion ...`. Não há ajuste de `sys.path` em `tests/conftest.py`; o `uv sync` resolve tudo.

## Arquitetura

**Três camadas.** `settings.py` (pydantic-settings, lê o `.env` da raiz ancorado em `REPO_ROOT`) → `core/` (biblioteca) → `pipelines/<domínio>/<fonte>/` (uma pasta por sistema de origem com **um** `<fonte>_etl.py` que faz o ETL de todas as entidades, parsers inclusos). Não crie `_common.py`/`_parsers.py`: o que é da fonte fica no `<fonte>_etl.py`, o que serve a mais de uma fonte vai para a `core`. Módulos separados só quando a estrutura das entidades muda muito, ainda com um único `<fonte>_etl.py` como entrypoint (`financas/investimentos`: b3/avenue/google).

**Um contrato: extract → transform → load, cada etapa lendo e escrevendo disco.** `extract(cfg)` grava arquivos brutos em `cfg.landing_dir`; `transform(cfg)` produz o bronze **sempre via `write_bronze(cfg, df)`** (ou `write_bronze_streaming` para milhares de arquivos): sanitiza colunas, remove CR/LF, grava `cfg.bronze_filepath` com `cfg.bronze_sep` (`;`); `load` vai para `raw_<fonte>.<tabela>`. As etapas são tasks separadas no Airflow, então nada passa em memória entre elas.

**YAML descreve o quê, Python descreve como, `core` faz o resto.** Cada fonte tem um `<fonte>_config.yml` com `db_schema`, opcionalmente `load`/`bronze_sep`/`options` no topo (default do arquivo, sobrescrevível por source), `write` (`truncate`|`append`), blocos `environments: {dev, prod}` (dev = execução local, prod = Airflow; só esses dois) (paths com `${LAKE_ROOT}`/`${SEEDS_ROOT}`) e `sources: {<entidade>: ...}`. `PipelineConfig.from_yaml(yml, source)` escolhe o ambiente por `settings.env` (var `ENV`); não passe `env=` em código de pipeline. `PipelineConfig` normaliza os diretórios para `Path`, resolve `${VAR}`, cria os diretórios (`criar_dirs=True`; em testes `False`) e substitui **apenas** `{date}` em `landing_file`/`bronze_file`; outros placeholders (`{id}`, `{day}`, `{game_id}`) ficam para o script resolver com `str.format`. Chaves que a `core` não interpreta vão em `options:` e chegam em `cfg.options`.

**`GenericETL`** (`core/etl.py`): `GenericETL(cfg, extract_fn=..., transform_fn=..., load_fn=None, log=logger)` + `run(steps=...)`. Defaults: extract baixa `cfg.url_base` para `cfg.landing_filepath` (sem `url_base`, no-op: landing alimentado por fora); transform é no-op (fontes JSON → JSONB); load segue `cfg.load` — `table` (bronze CSV inteiro → `COPY`), `files` (cada CSV do bronze_dir → tabela de mesmo nome), `jsonb` (JSONs do landing → `JsonbLoader`, com `options.array_key/overwrite/control_table/file_pattern`), `none` (orquestrador carrega). **Como** escrever vem de `cfg.write`: `truncate` (default, full refresh por `TRUNCATE` + `COPY`) ou `append`; não há upsert. A carga é sempre `COPY` numa transação e nunca `DROP` — a tabela não é recriada, então as views do dbt sobrevivem e o OID fica estável. `write: append` + `options.control_table` é a carga incremental por arquivo (`core/control.py`): o transform termina em `write_bronze_incremental`, o bronze vira o delta e o load registra o manifesto na mesma transação do COPY. Ao ligar numa tabela com histórico, rode `uv run python scripts/controle_semear.py <config.yml> <entidade>` uma vez. Sem `db_schema` `raw_*` levanta `ValueError`. Todo `<fonte>_etl.py` define `ETLS = {"<entidade>": Etl(extract=..., transform=..., load=...)}` (chave = source do YAML, ordem = ordem de execução; variação entre entidades via `functools.partial`) e termina em `run_source(CONFIG_FILE, ETLS)`, que dá `[entidade ...] --steps`, configura o log e isola falhas quando roda várias. DAGs/testes usam `build_etl(CONFIG_FILE, "<entidade>", ETLS["<entidade>"])`.

**Encadeamento por parâmetros.** Produtor declara `output_param_file` (`{arquivo: coluna}`) e chama `cfg.write_output_params(df, default_column=...)`; consumidor declara `parameter_file`. Isso define a ordem de execução, documentada no README da fonte. Incremental por ID: `core.pending_ids`/`mark_no_data` (todos os IDs − já no landing − CSV "sem dados"); `core.extract_by_ids(cfg, has_data=...)` faz o loop a partir de `{id}` em `base_url`/`landing_file` e `options.no_data_file`. Incremental por data: `core.missing_dates_from_db`.

**Exceções ao `GenericETL`** (não escrevem em `raw_*`): `precos/atacadao` (CSV → seed do dbt), `financas/fundos_imobiliarios` (CLI mensal + relatório), `financas/investimentos/investimentos_fgc.py` (DW → seed). Mesmo aí, use `HttpClient`, `PostgresClient.read_sql`, `write_csv`, `concat_files_to_df`, `setup_logger()`. Tudo o mais está no padrão, inclusive NHL (`load: jsonb`) e solar/openweather (`load: table`, full refresh a partir do landing; a DAG só chama `build_etl` e roda as etapas). `load: none` é só de `investimentos/fgc` e `vide_editorial/categorias`, que não carregam em `raw_*`.

**Um banco por ambiente, um schema por fonte.** `ENV` escolhe o perfil de conexão `DB__<ENV>__*` do `.env` (`settings.db_target`); `PostgresClient()` sem argumentos usa esse perfil; no Airflow, `PostgresClient(connection=hook.get_conn())`. Em `ENV=dev` (local) o banco é obrigatoriamente `ingestion_sandbox` (validator em `settings.py`), para os testes de carga não sujarem a raw que o dbt consome no `analytics_dev`; `ENV=prod` é o Airflow, produção em outro host. Nunca passe `db_name=` para desviar de banco. Toda escrita exige `schema=` explícito começando com `raw_` (`core.db.validate_raw_schema`; `copy_csv`, `load_files_to_table`, `JsonbLoader` não têm default). O schema de uma fonte é declarado uma vez, na chave `db_schema` do topo do `<fonte>_config.yml`. Leituras de objetos do dbt (`intermediate.int_renda_fixa`, hoje só em `investimentos_fgc`) usam `PostgresClient(settings.models_target)`: em dev é o `analytics_dev`, em prod o mesmo banco da carga. Nenhum pipeline depende de uma execução do dbt para saber o que rodar — a NHL descobre os IDs pendentes consultando a própria `raw_nhl`. `scripts/raw_copy.sh {seed|promote|pull} raw_<fonte>` copia schemas raw tabela a tabela: `seed` (`analytics_dev` → sandbox), `promote` (sandbox → `analytics_dev`) e `pull` (`analytics_prod`, perfil `DB__PROD__*`, → `analytics_dev`), que é como a raw que o dbt de dev consome deixa de envelhecer. A cópia é `TRUNCATE` + `COPY`, nunca `DROP`, então as views do dbt sobrevivem. Cada tabela vai em duas etapas e é atômica: a origem escreve num arquivo (se ela falhar, o destino nem foi tocado) e o destino carrega numa transação só — `TRUNCATE`, `COPY` e a conferência do número de linhas, que dá `ROLLBACK` se o que entrou não bater com o que a origem escreveu (é o que pega o stream cortado no meio, que viraria buraco silencioso no delta). Custa um pico de disco em `$TMPDIR` (`RAW_COPY_TMP` muda o lugar) e o `ACCESS EXCLUSIVE` do `TRUNCATE` até o `COMMIT`.  por tabela ele escolhe sozinho entre completa e só o delta (`loaded_at_utc` > o máximo do destino), caindo na completa quando falta `loaded_at_utc`, quando o destino está vazio, quando a origem reescreveu tudo (full refresh) ou quando há índice único. `--dry-run` mostra a decisão; `--full` força a cópia completa. As credenciais saem do `.env` da raiz, mas a variável de ambiente de mesmo nome vence o arquivo e dispensa o `.env` — é assim que a DAG `raw_pull_prod` do my_orchestrator roda o `pull` de hora em hora de dentro do container do Airflow, onde só existem as `DB__*` do ambiente. A lista de colunas vem da origem e é usada nos dois lados, então os bancos têm de estar no mesmo nome de coluna; divergência dá erro de `\copy`, não cópia parcial silenciosa.

**`HttpClient` devolve `None` em erro** em vez de levantar exceção (`get_json`, `get_text`, `request`, `fetch_and_save*`). Trate o item, logue e siga; um ID quebrado não pode derrubar uma extração longa.

**Nomes de tabela e pastas do lake preservados por compatibilidade com o dbt (`my_analytics`)/Airflow:** `raw_<fonte>.<entidade>` é só o padrão para fonte **nova**; quando já existe consumidor, `db_table`/`db_schema` seguem o nome que o `my_analytics` lê em `models/staging/_sources.yml`. Hoje: `raw_camara.raw_camara_*` (entidade `votos_orientacao` → `raw_camara_votacoes_orientacao`), `raw_senado.raw_senado_*`, `raw_ecidadania.raw_ecidadania_*`, `raw_radar_congresso.raw_radar_*`, `raw_ranking_politicos.raw_ranking_*`, `raw_google_sheets.*` (investimentos/google), `raw_vide_editora.vide_raw_home_featured`, `raw_apsystem.solar_daily_energy|solar_hourly_energy`, `raw_openweather.openweather_daily`, `raw_nhl.nhl_raw_*`, `raw_nhl.nhl_ingestion_control`; e as pastas `raw/nhl/*`, `raw/demodados/*`, `raw/vide`. Não renomeie. O `bronze_sep: ","` de clima e solar é o formato histórico do CSV deles; nada fora dali o lê.

## Convenções que importam ao editar

- Credencial nova: `.env` **e** `.env.example` (valor vazio) **e** campo em `settings.py`. Conexão de banco só via perfil `DB__<ENV>__*` (`DbTarget`). Nunca `os.getenv` espalhado. Campo obrigatório em `Settings` não leva default. Arquivo de credencial mora em `~/.secrets/`, o `.env` guarda só o caminho. Grupos de valores usam delimitador duplo (`URL_FINANCE__<CHAVE>` → `settings.url_finance`).
- Nunca caminho absoluto em YAML ou código; use `${LAKE_ROOT}`/`${SEEDS_ROOT}` (em Python, `settings.lake_root`/`settings.seeds_root`). Não há mais exceção.
- Raw é sempre texto: a carga grava toda coluna como `TEXT`, por `COPY` do bronze CSV. Não crie caminho de carga que infira tipos; a tipagem é do dbt. O `JsonbLoader` (payload `JSONB`) é a exceção JSON. `arquivo_origem` e `loaded_at_utc` não vêm do dado: são `DEFAULT` no catálogo, e `loaded_at_utc` é o `now() AT TIME ZONE 'utc'` da transação — um valor só por carga, em UTC independente do fuso do servidor. Tabela anterior ao carimbo ganha a coluna na carga seguinte (`core.db.ensure_loaded_at`).
- Bronze via `write_bronze`/`write_bronze_streaming` (gravam float inteiro como inteiro, sem `.0`); nomes de coluna vêm de `sanitize_columns` (aplicado ali). A exceção é `load: files`, em que cada CSV vira uma tabela: `financas/investimentos` usa `write_csv` + `integral_floats_to_int` direto. `sanitize_values(df, exclude=[...])` quando precisar normalizar valores preservando URLs/e-mails.
- Cascata de nomes: pasta `<fonte>/`, script `<fonte>_etl.py`, source YAML = chave de `ETLS` = `<entidade>`, schema `raw_<fonte>` (chave `db_schema` no topo do YAML), tabela `<entidade>`. Schema e tabela são orientação: quando a tabela já existe para o `my_analytics`, use o nome dela. Deixe no fim do `__main__` o comentário com o comando `uv run python -m ... [entidade ...] [--steps ...]`.
- Logger: módulo usa `logging.getLogger(__name__)`; só o entrypoint chama `setup_logger()` (o `run_source` já faz). Nunca `logging.basicConfig`.
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
