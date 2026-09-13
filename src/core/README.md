# `core/` — biblioteca compartilhada

Tudo que mais de um pipeline precisa mora aqui: cliente HTTP, carga no Postgres,
configuração por YAML, orquestração das três etapas, escrita do bronze e limpeza
de texto. A regra é **um jeito de fazer cada coisa**; helper que serve a um domínio
só fica em `pipelines/<domínio>/<fonte>/_common.py`.

**Regra prática:** se você está prestes a escrever `requests.Session()`,
`create_engine(...)`, `yaml.safe_load(...)`, `to_csv(...)` de bronze ou
`logging.basicConfig(...)` dentro de um pipeline, já existe aqui.

Tudo que um pipeline usa é reexportado no pacote:

```python
from core import (
    GenericETL,
    run_cli,
    run_many,  # etl.py
    PipelineConfig,
    load_yaml,  # config.py
    HttpClient,  # http.py
    PostgresClient,
    validate_raw_schema,  # db.py
    JsonbLoader,  # jsonb.py
    write_bronze,
    write_bronze_streaming,  # io.py
    list_files,
    concat_files_to_df,
    write_csv,  # io.py
    missing_dates,
    missing_dates_from_db,  # incremental.py (por data)
    pending_ids,
    mark_no_data,  # incremental.py (por ID)
    sanitize_columns,
    sanitize_values,
    normalize_string,  # text.py
    setup_logger,  # logging.py
)
```

Parsers vêm do submódulo: `from core.parsers.json import normalize_json_object`,
`from core.parsers.html import make_bs_object`.

---

## O contrato

Todo pipeline é **extract → transform → load**, e cada etapa lê e escreve disco
(no Airflow elas são tasks separadas; nada passa em memória entre elas):

| Etapa | Escreve em | Default da `core` (sem função sua) |
|---|---|---|
| `extract(cfg)` | `cfg.landing_dir` (JSON/HTML/CSV bruto) | baixa `cfg.url_base` para `cfg.landing_filepath`; sem `url_base`, no-op (landing alimentado por fora) |
| `transform(cfg)` | `cfg.bronze_filepath` (CSV, `cfg.bronze_sep`), sempre via `write_bronze` | no-op (fontes JSON → JSONB) |
| `load()` | `raw_<fonte>.<tabela>` | por `cfg.load`: `table` \| `files` \| `jsonb` \| `none` |

```python
import logging
from pathlib import Path

from core import GenericETL, PipelineConfig, run_cli, write_bronze

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "camara_config.yml"


def extract(cfg: PipelineConfig) -> None: ...  # arquivos em cfg.landing_dir
def transform(cfg: PipelineConfig) -> None:  # termina em write_bronze
    write_bronze(cfg, df)


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "deputados")
    return GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.camara.camara_deputados [--steps transform,load]
```

---

## `etl.py`

### `GenericETL(cfg, *, extract_fn=None, transform_fn=None, load_fn=None, log=None)`

- `run(steps=("extract", "transform", "load"))` executa as etapas pedidas, na ordem
  canônica; nome inválido levanta `ValueError`.
- `extract()` / `transform()` / `load()` chamam a função sua ou o default da tabela
  acima. `load_fn` sobrescreve o modo do YAML (uso raro: NHL `player_game_log`).
- Modos de load:
  - `table` — lê o bronze em chunks de 50 mil linhas (`sep=cfg.bronze_sep`) e grava
    em `<db_schema>.<db_table>` com `replace` (1º chunk) + `append`. CSV só com
    cabeçalho cria a tabela vazia.
  - `files` — `PostgresClient.load_files_to_table(cfg.bronze_dir, ...)`: cada CSV
    (`options.file_pattern`, default `*.csv`) vira a tabela de mesmo nome.
  - `jsonb` — `JsonbLoader` sobre `cfg.landing_dir.glob(options.file_pattern or
    landing_file)`, com `options.array_key`, `options.overwrite` e
    `options.control_table` (default `ingestion_control`).
  - `none` — só loga (o orquestrador carrega).
- `db_schema` ausente ou sem prefixo `raw_` levanta `ValueError` antes de conectar.

### `run_cli(build, argv=None)`
Entrypoint padrão: `setup_logger()` + `--steps a,b` + `build().run(steps)`.

### `run_many(pipelines: dict[str, Callable], only=None)`
Roda vários pipelines isolando falhas (loga, segue, `sys.exit(1)` no fim se alguma
falhou). `only` roda um só. Usado por `nhl/run_all.py` e `investimentos/run_all.py`.

---

## `config.py`

### `PipelineConfig`

Dataclass com os caminhos e nomes de um source. `PipelineConfig.from_yaml(config_file,
source, *, env=None, **overrides)` é a forma de construir (o `env` default vem de
`settings.env`; `overrides` servem para `criar_dirs=False` em testes).

| Campo | Uso |
|---|---|
| `landing_dir` | **obrigatório** — diretório do dado bruto |
| `bronze_dir` | diretório do CSV pós-transformação |
| `parameter_dir` | diretório dos CSVs de parâmetros (entrada e saída) |
| `url_base` | URL da fonte (aceita placeholders que o pipeline resolve com `str.format`) |
| `subpath` | subpasta aplicada a landing/bronze |
| `landing_file` / `bronze_file` | nomes de arquivo; `{date}` vira a data de hoje, outros placeholders são preservados |
| `parameter_file` | CSV de entrada que parametriza a extração |
| `output_param_file` | `str` ou `{arquivo: coluna}` gerado para o próximo pipeline (em `parameter_dir`) |
| `db_table` / `db_schema` | tabela e schema destino (`raw_<fonte>`) |
| `load` | `table` (default) \| `files` \| `jsonb` \| `none` |
| `bronze_sep` | separador do bronze (default `;`) |
| `options` | dict livre do bloco `options:` (a core lê só as chaves listadas em `etl.py`) |
| `criar_dirs` | cria os diretórios no `__init__` (default `True`; em testes use `False`) |

Propriedades `landing_filepath`, `bronze_filepath`, `parameter_filepath` levantam
`ValueError` com mensagem clara se o campo não foi configurado.
`write_output_params(df, default_column=None)` exporta os valores únicos de uma
coluna (ou de várias, com o dict) para `parameter_dir`.

Formato do YAML (`db_schema`, `load`, `bronze_sep` e `options` valem no topo como
default do arquivo e podem ser sobrescritos por source; `options` faz merge):

```yaml
db_schema: "raw_camara"
load: table                       # opcional
environments:
  local:
    base_raw: "${LAKE_ROOT}/raw/demodados/camara"
    base_bronze: "${LAKE_ROOT}/bronze/demodados/camara"
    base_parameters: "${LAKE_ROOT}/raw/demodados/camara/parameters"
  airflow:
    base_raw: "/usr/local/airflow/mylake/raw/demodados/camara"
sources:
  votos_deputados:
    base_url: "https://dadosabertos.camara.leg.br/api/v2/votacoes/{id}/votos"
    subpath: "votos_deputados"
    landing_file: "{id}_votos_deputados.json"
    bronze_file: "camara_votos_deputados.csv"
    db_table: "votos_deputados"
    parameter_file: "id_votacoes.csv"
    options:
      no_data_file: "sem_dados_id_votacao.csv"
```

### `load_yaml(path) -> dict`
Única forma de ler YAML no projeto.

---

## `http.py` — `HttpClient`

`requests.Session` com retry e backoff exponencial (403, 429, 5xx; GET e POST).
Nenhum método levanta exceção de rede: **logam e devolvem `None`**. Sempre teste o
retorno.

```python
HttpClient(log=None, retries=5, backoff_factor=2.0, timeout=30, headers=None)
# scraping de site: HttpClient(logger, retries=3, backoff_factor=0.5, timeout=10)
```

| Método | Para quê |
|---|---|
| `request(url, *, method="GET", mode="auto", headers=None, data=None, timeout=None, **kw)` | Requisição genérica; `mode` = `json` \| `text` \| `auto` (decide pelo Content-Type) |
| `get_json(url, **kw)` | GET que espera JSON (`params=` para query string) |
| `get_text(url, **kw)` | GET que devolve HTML/texto, com User-Agent de browser |
| `save_json(data, output_dir, filename)` | Grava JSON indentado (sufixo `.json` garantido); `None` se `data` for `None` |
| `fetch_and_save(url, output_dir, filename)` | `get_json` + `save_json` |
| `fetch_and_save_many(tasks, output_dir, workers=1)` | Lista de `(url, filename)`; threads se `workers > 1` |

---

## `db.py` — `PostgresClient`

A conexão vem de `settings.db_target` (perfil `DB__<ENV>__*` do `.env`) ou de um
`DbTarget`, `connection` ou `engine` injetado — no Airflow,
`PostgresClient(connection=hook.get_conn())`. Com injeção a `core` nem importa o
`settings`.

**Schema é obrigatório em toda escrita** e precisa ser `raw_<fonte>`
(`validate_raw_schema`). Todo carregamento acrescenta `arquivo_origem` (quando você
passa `filename`) e `data_carga`.

| Método | Para quê |
|---|---|
| `send_df_to_db(df, table_name, *, schema, how="replace", filename=None)` | Grava um DataFrame |
| `load_files_to_table(input_dir, *, schema, table_name=None, pattern="*.csv", how="replace", source_column="arquivo_origem", sep=";")` | Diretório inteiro: com `table_name`, tudo numa tabela; sem, uma tabela por arquivo (stem) |
| `read_sql(sql)` | Resultado como DataFrame (leituras em `staging.*`, `intermediate.*`) |
| `connect()` | Conexão psycopg2 crua (`copy_expert`, transação explícita) |
| `alchemy()` | Engine SQLAlchemy |

---

## `io.py`

```python
write_bronze(cfg, df) -> Path | None
```
**A forma de terminar um transform.** Sanitiza nomes de coluna
(`sanitize_columns`), remove CR/LF dos valores texto (`strip_newlines`), cria o
diretório e grava `cfg.bronze_filepath` com `cfg.bronze_sep`. DataFrame vazio ou
`None`: warning, não grava, bronze anterior preservado.

```python
write_bronze_streaming(cfg, files, parse_fn) -> Path | None
```
Mesma coisa, um arquivo por vez (memória limitada): `parse_fn(file)` devolve o
DataFrame daquele arquivo (ou `None` para pular); cabeçalho fixado no primeiro,
demais alinhados (colunas extras descartadas com warning); exceção num arquivo é
logada e pulada; escrita em temporário + `os.replace`. É um **rebuild** completo:
quem quiser incrementalidade filtra `files` antes.

```python
list_files(input_dir, pattern="*") -> list[Path]                   # rglob ordenado
concat_files_to_df(input_dir, pattern="*.csv", sep=",", source_column=None) -> DataFrame
write_csv(df, output_dir, filename, sep=";") -> Path               # seeds, exceções
```

---

## `incremental.py`

Por data (solar, openweather):

```python
max_date(db: PostgresClient, sql) -> date | None
missing_dates(since, cutoff_hour=20, now=None) -> list[str]      # since+1 .. ontem (hoje se >= 20h)
missing_dates_from_db(db, sqls, control_path, cutoff_hour=20) -> list[str]
write_dates_csv(dates, path) / read_dates_csv(path)
```
`missing_dates_from_db` parte do **menor** high-water mark entre os `sqls` (cada um
devolve `MAX(data)` de uma tabela), levanta `ValueError` se algum for nulo e grava o
CSV de controle.

Por ID (legislativo):

```python
pending_ids(all_ids, done_ids, no_data_path=None) -> list[str]   # preserva a ordem
mark_no_data(no_data_path, id_) -> None                          # CSV com header "id"
```
O padrão dos "três conjuntos": todos os IDs menos os já no landing menos os que a
API nunca respondeu. `pipelines/legislativo/_common.extract_by_ids` monta tudo a
partir do YAML.

---

## `jsonb.py` — `JsonbLoader`

Carga de JSON bruto em tabela `(payload JSONB, source_filename TEXT)` via `COPY`,
para fontes cuja normalização fica no dbt (NHL). O `GenericETL` chama isto no modo
`load: jsonb`; use direto só para recargas manuais.

```python
JsonbLoader(db, *, schema, control_table="ingestion_control", log=None)
    .load_files(files, table, array_key=None, overwrite=False) -> int
json_file_to_ndjson_buffer(path, array_key=None, source_filename=None)  # função pura
```
`overwrite=True`: `TRUNCATE` + recarga; `False`: só arquivos ausentes da tabela de
controle. Tudo numa transação.

---

## `text.py`

```python
sanitize_columns(df, cols=None, *, case="lower", space="replace", alfanum="replace") -> DataFrame
sanitize_values(df, *, exclude=(), case="upper", space="keep", alfanum="remove") -> DataFrame
strip_newlines(df) -> DataFrame
normalize_string(s) -> str        # "Ações Ordinárias" -> "acoes_ordinarias"
```
Todas devolvem cópia. `write_bronze` já aplica `sanitize_columns` e
`strip_newlines`; chame `sanitize_columns` explicitamente só quando a lógica do
transform depende do nome sanitizado, e `sanitize_values(exclude=[...])` para
normalizar valores preservando URLs/e-mails/datas.

---

## `logging.py`

```python
setup_logger(name=None, level=logging.INFO, log_file=None) -> Logger
```
Configura o logger **raiz** uma vez (console e, opcionalmente, arquivo rotativo) e
devolve `getLogger(name)`. Regra: módulos usam `logging.getLogger(__name__)`; o
entrypoint chama `setup_logger()` — `run_cli` e `run_many` já fazem isso. Assim o
`INFO` de qualquer módulo chega ao console, sem depender da hierarquia de nomes.

---

## `parsers/`

```python
normalize_json_object(filepath, key=None) -> DataFrame   # pd.json_normalize(sep=".")
make_bs_object(input_file=None, response=None) -> BeautifulSoup
```

---

## Convenções ao evoluir esta pasta

- Nada de credencial ou caminho absoluto: use `settings` e `${LAKE_ROOT}` no YAML.
- Um jeito por coisa. Antes de adicionar uma função, veja se é variação de uma que
  existe (parâmetro) ou se serve a um domínio só (`_common.py` do domínio).
- Toda função pública com docstring — é o que aparece nesta referência.
- Cuidado com nomes de topo: `src/` é a raiz de código, então um `src/novo.py` vira
  o módulo global `novo` dentro do venv.
- Testes da lib ficam em `tests/core/`; rode com `uv run task test`.
