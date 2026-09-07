# `core/` — biblioteca compartilhada

Tudo que mais de um pipeline precisa mora aqui: cliente HTTP, carga no Postgres,
configuração por YAML, orquestração ETL, leitura/escrita de arquivos e limpeza de
texto. Nasceu da consolidação dos projetos antigos (havia 5 extractors HTTP, 3
loggers e 4 loaders Postgres duplicados).

**Regra prática:** se você está prestes a escrever `requests.Session()`,
`create_engine(...)`, `yaml.safe_load(...)` ou `logging.basicConfig(...)` dentro de
um pipeline, provavelmente já existe aqui.

Os nomes mais usados são reexportados no pacote:

```python
from core import (
    ColumnSanitizer,
    GenericETL,
    HttpClient,
    PipelineConfig,
    PostgresClient,
    concat_files_to_df,
    expand_path,
    list_files,
    load_source_config,
    load_yaml,
    normalize_string,
    setup_logger,
    write_csv,
)
```

Os demais vêm do submódulo (`from core.parsers.html import make_bs_object`).

| Módulo | O que oferece |
|---|---|
| `logging.py` | `setup_logger` |
| `config.py` | `PipelineConfig`, `load_source_config`, `load_yaml`, `expand_path` |
| `etl.py` | `GenericETL` |
| `http.py` | `HttpClient` |
| `db.py` | `PostgresClient` |
| `io.py` | `list_files`, `concat_files_to_df`, `write_csv` |
| `text.py` | `normalize_string`, `ColumnSanitizer` |
| `parsers/json.py` | `make_df_from_json_list`, `normalize_json_object` |
| `parsers/html.py` | `make_bs_object` |

---

## O pipeline típico

A maioria dos pipelines segue este esqueleto — YAML descreve *o quê* ingerir, o
código descreve *como*, e o `GenericETL` costura as três etapas:

```python
from pathlib import Path
from core import GenericETL, PipelineConfig, load_source_config, setup_logger

logger = setup_logger(__name__)
_CONFIG_FILE = Path(__file__).parent / "camara_config.yml"


def extract(cfg: PipelineConfig) -> Path: ...
def transform(cfg: PipelineConfig) -> None: ...


def run_pipeline(cfg: PipelineConfig) -> None:
    GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger).run()


if __name__ == "__main__":
    cfg = load_source_config(_CONFIG_FILE, source="deputados")
    run_pipeline(PipelineConfig(**cfg))
```

Roda com `uv run python -m pipelines.legislativo.camara.camara_deputados`.

---

## `logging.py`

```python
setup_logger(name="my_ingestion", level=logging.INFO, log_file=None) -> Logger
```

Logger único do monorepo, formato `data | nome | nível | mensagem`. É idempotente:
chamar duas vezes com o mesmo nome **não** duplica handlers. Passe `log_file` para
também gravar em arquivo rotativo (10 MB, 5 backups).

```python
logger = setup_logger(__name__)
logger = setup_logger("solar", log_file=settings.lake_root / "logs" / "solar.log")
```

Não use `logging.basicConfig()` em nível de módulo: ele configura o root logger e
afeta todo o processo que importar o arquivo. Os pipelines migrados do
`legislativo` ainda o chamam dentro do `__main__` (onde o efeito colateral é
inofensivo, pois o script *é* o processo) — em código novo, prefira `setup_logger`
nos dois lugares.

---

## `config.py`

### `load_yaml(path) -> dict`
Única forma de ler YAML no projeto (`yaml.safe_load` com encoding explícito).

### `expand_path(value) -> str`
Resolve `${VAR}` e `~` num caminho vindo de YAML. Além das variáveis de ambiente,
entende `${LAKE_ROOT}` e `${SEEDS_ROOT}` a partir do `settings`. É por isso que
nenhum YAML tem caminho absoluto hardcoded.

### `PipelineConfig`
Dataclass com todos os caminhos e nomes de um pipeline. Campos:

| Campo | Uso |
|---|---|
| `landing_dir` | **obrigatório** — diretório do dado bruto (JSON/HTML) |
| `bronze_dir` | diretório do dado pós-transformação (CSV) |
| `db_table` | tabela destino em `raw.*` |
| `url_base` | URL da fonte |
| `subpath` | subpasta aplicada a landing/bronze/error |
| `error_dir` | diretório de fallback |
| `landing_file` / `bronze_file` | nomes de arquivo; aceitam o template `{date}` |
| `parameter_dir` / `parameter_file` | CSV de entrada que parametriza a extração |
| `output_param_dir` / `output_param_file` | CSV de IDs gerado para o próximo pipeline |
| `criar_dirs` | cria os diretórios no `__init__` (default `True`) |

No `__post_init__` ele resolve `${VAR}`, aplica `subpath`, troca `{date}` pela data
de hoje, deriva `bronze_file` a partir de `landing_file` (trocando a extensão para
`.csv`) e cria os diretórios.

> **Atenção:** o construtor cria diretórios por padrão. Em testes passe
> `criar_dirs=False` para não sujar o disco.

As propriedades `landing_filepath`, `bronze_filepath`, `parameter_filepath` e
`output_param_filepath` juntam diretório + arquivo e levantam `ValueError` com
mensagem clara se o campo não foi configurado.

`write_output_params(df, default_column=..., logger=...)` exporta os valores
únicos de uma coluna para o CSV de parâmetros. Se `output_param_file` for um dict
`{arquivo: coluna}`, exporta várias saídas de uma vez.

### `load_source_config(config_path, source, env=None) -> dict`
Lê o YAML da fonte e devolve um dict pronto para `PipelineConfig(**cfg)`. O `env`
default vem de `settings.env` (variável `ENV` do `.env`). Formato esperado:

```yaml
environments:
  local:
    base_raw: "${LAKE_ROOT}/raw/demodados/camara"
    base_bronze: "${LAKE_ROOT}/bronze/demodados/camara"
    base_parameters: "${LAKE_ROOT}/raw/demodados/camara/parameters"
  airflow:
    base_raw: "/usr/local/airflow/mylake/raw/demodados/camara"
    # ...

sources:
  deputados:
    base_url: "https://dadosabertos.camara.leg.br/api/v2/deputados/"
    subpath: "deputados"
    bronze_file: "parlamento_deputados.csv"
    db_table: "raw_camara_deputados"
    parameter_file: "id_deputados.csv"
```

---

## `http.py` — `HttpClient`

`requests.Session` com retry e backoff exponencial já montados (repete em 403, 429,
500, 502, 503, 504, para GET e POST).

```python
HttpClient(log=None, retries=5, backoff_factor=2.0, timeout=30, headers=None)
```

Para scraping de site (mais frágil e mais lento), o padrão adotado nos pipelines é
`HttpClient(logger, retries=3, backoff_factor=0.5, timeout=10)`.

| Método | Para quê |
|---|---|
| `make_request(url, method="GET", headers=None, data=None, mode="auto", timeout=None)` | Requisição genérica. `mode="json"`, `"text"` ou `"auto"` (decide pelo Content-Type) |
| `make_http_request(url, method="GET")` | Atalho que sempre espera JSON |
| `make_http_request_text(url)` | Atalho para HTML/texto, já com User-Agent de browser |
| `save_json(data, output_dir, filename)` | Grava JSON indentado; garante o sufixo `.json` e cria o diretório (estático) |
| `save_response(data, output_dir, filename)` | Igual, usando o logger da instância |
| `fetch_and_save(url, output_dir, filename)` | Requisita JSON e grava em disco |
| `fetch_and_save_many(tasks, output_dir, workers=1)` | Lista de `(url, filename)`; com `workers > 1` usa ThreadPool |

Nenhum método levanta exceção de rede: em caso de erro, logam e devolvem `None`
(ou pulam o item, no `fetch_and_save_many`). **Sempre teste o retorno.**

```python
http = HttpClient(logger)

data = http.make_http_request("https://api.exemplo/v1/itens")
if data is None:
    logger.warning("sem dados")
    return

html = http.make_http_request_text("https://site.exemplo/pagina")

tasks = [(f"{url}/{i}", f"item_{i}") for i in ids]
http.fetch_and_save_many(tasks, cfg.landing_dir, workers=8)
```

---

## `db.py` — `PostgresClient`

Credenciais vêm do `settings` (`.env` da raiz); dá para sobrescrever por argumento
ou injetar `connection`/`engine` prontos (útil em teste). Levanta `ValueError` na
construção se não houver credencial nenhuma. O schema default é `raw`.

Todo carregamento acrescenta as colunas de rastreio **`arquivo_origem`** (quando
você passa `filename`) e **`data_carga`**.

| Método | Para quê |
|---|---|
| `send_df_to_db(df, table_name, how="replace", filename=None, schema="raw")` | Grava um DataFrame |
| `send_csv_to_db(csv_path, table_name, sep=";", chunksize=50_000, how="replace")` | CSV grande em streaming; respeita o limite de 65535 parâmetros do Postgres e cria a tabela mesmo se o CSV só tiver cabeçalho |
| `load_files_to_table(input_dir, table_name=None, file_extension="csv", pattern=None, strip_prefix="", source_column="arquivo_origem", sep=",")` | Carrega um diretório inteiro (ver os dois modos abaixo) |
| `execute_query(sql)` | Executa DDL/DML |
| `fetchone(sql)` | Uma linha (ex.: high-water mark) |
| `fetchall(sql)` | Resultado completo como DataFrame |
| `alchemy()` | Engine SQLAlchemy, se precisar de algo fora da API |

`load_files_to_table` tem **dois modos**, decididos por `table_name`:

```python
db = PostgresClient(log=logger)

# 1) todos os arquivos numa tabela só, rastreados por source_column
db.load_files_to_table(raw_dir, table_name="vide_raw_destaques", file_extension="json")

# 2) table_name=None -> cada arquivo vira uma tabela, nomeada pelo stem do arquivo
#    (consolidado_acoes.csv -> raw.acoes)
db.load_files_to_table(bronze_dir, strip_prefix="consolidado_")
```

> Cargas são **full refresh** (`how="replace"`) por padrão: os volumes são pequenos
> e a idempotência vem de recarregar tudo. Use `how="append"` conscientemente.

---

## `etl.py` — `GenericETL`

Orquestra `extract → transform → load`. Cada etapa aceita uma função sua; sem ela,
cai no comportamento padrão.

```python
GenericETL(cfg, extract_fn=None, transform_fn=None, load_fn=None, log=None)
```

- `extract()` — sem `extract_fn`, baixa `cfg.url_base` para `cfg.landing_filepath`
  via `HttpClient`.
- `transform()` — **não tem default**: sem `transform_fn` levanta
  `NotImplementedError`. É sempre específico da fonte.
- `load()` — sem `load_fn`, lê `cfg.bronze_filepath` (CSV `;`) e manda para
  `raw.<cfg.db_table>` com `replace`.

`run()` chama as três em ordem. Você também pode chamar etapas isoladas — vários
pipelines fazem só `etl.transform()` + `etl.load()` porque a extração é paginada e
tem lógica própria.

---

## `io.py`

```python
list_files(input_dir, pattern="*") -> list[Path]
```
Lista recursiva (`rglob`), ordenada e com caminhos resolvidos.

```python
concat_files_to_df(input_dir, pattern="*.csv", sep=",",
                   source_column=None, dedup_key=None, existing=None) -> DataFrame
```
Concatena CSV/JSON de um diretório. `source_column` adiciona o nome do arquivo de
origem; `dedup_key` remove duplicatas mantendo a última ocorrência; `existing`
anexa a um DataFrame já consolidado antes de deduplicar — a combinação dos dois
dá consolidação idempotente:

```python
# reprocessar o mesmo mês não duplica linhas
df = concat_files_to_df(mes_dir, dedup_key="extraction_month", existing=historico)
```

```python
write_csv(df, output_dir, filename, sep=";") -> Path
```
Grava criando o diretório e garantindo o sufixo `.csv`. Note que o separador
default é `;` (padrão do bronze), enquanto o de leitura do `concat_files_to_df` é `,`.

---

## `text.py`

```python
normalize_string(s) -> str
```
Remove acentos, passa para minúsculas e colapsa espaços/hífens/underscores em `_`.
Use para nomes de coluna, de aba e de arquivo: `"Ações Ordinárias"` → `"acoes_ordinarias"`.

```python
ColumnSanitizer(df)
    .sanitize_columns_names(cols=None, case="lower", space="replace", alfanum="replace")
    .sanitize_columns_values(cols=None, case="upper", space="keep", alfanum="remove")
    .not_sanitize_columns_values(cols=[...])   # aplica a TODAS menos essas
    .df                                        # <- pega o resultado
```

API encadeável que trabalha sobre uma **cópia** do DataFrame; colunas numéricas são
ignoradas na limpeza de valores. Lembre de terminar com `.df` — sem isso você fica
com o sanitizer, não com o DataFrame.

```python
df = ColumnSanitizer(df).sanitize_columns_names().df
# preserva o case original de 'link' e 'url', normaliza o resto
df = ColumnSanitizer(df).not_sanitize_columns_values(["link", "url"]).df
```

---

## `parsers/`

```python
make_df_from_json_list(filepath, list_key) -> DataFrame
```
DataFrame a partir de uma lista sob `list_key`. Chave ausente → DataFrame vazio + warning.

```python
normalize_json_object(filepath, key=None) -> DataFrame
```
Achata JSON aninhado com `pd.json_normalize` (separador `.`).

```python
make_bs_object(input_file=None, response=None) -> BeautifulSoup
```
Soup com `html.parser`, a partir de **um** arquivo **ou** de texto de resposta.
Passar os dois (ou nenhum) levanta `ValueError`.

---

## Convenções ao evoluir esta pasta

- Nada de credencial ou caminho absoluto: use `settings` e `${LAKE_ROOT}` no YAML.
- Um módulo por responsabilidade; se algo serve a um pipeline só, ele fica na pasta
  do pipeline, não aqui.
- Toda função pública com docstring — é o que aparece no `help()` e nesta referência.
- Cuidado com nomes de topo: `src/` é a raiz de código, então um `src/novo.py` vira
  o módulo global `novo` dentro do venv.
- Testes da lib ficam em `tests/core/`; rode com `uv run task test`.
