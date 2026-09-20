# `core/` — biblioteca compartilhada

Tudo que mais de um pipeline precisa mora aqui: cliente HTTP, carga no Postgres,
configuração por YAML, orquestração das três etapas, escrita do bronze e limpeza
de texto. A regra é **um jeito de fazer cada coisa**; helper que serve a uma fonte
só fica no próprio `pipelines/<domínio>/<fonte>/<fonte>_etl.py`.

**Regra prática:** se você está prestes a escrever `requests.Session()`,
`create_engine(...)`, `yaml.safe_load(...)`, `to_csv(...)` de bronze ou
`logging.basicConfig(...)` dentro de um pipeline, já existe aqui.

Tudo que um pipeline usa é reexportado no pacote:

```python
from core import (
    Etl,
    GenericETL,
    build_etl,
    run_source,  # etl.py
    PipelineConfig,
    load_yaml,  # config.py
    HttpClient,  # http.py
    PostgresClient,
    validate_raw_schema,  # db.py
    JsonbLoader,  # jsonb.py
    write_bronze,
    write_bronze_streaming,  # io.py
    reset_bronze,
    list_files,
    concat_files_to_df,
    concat_landing,
    write_csv,  # io.py
    missing_dates,
    missing_dates_from_db,  # incremental.py (por data)
    pending_ids,
    mark_no_data,
    read_ids,
    landing_ids,
    extract_by_ids,  # incremental.py (por ID)
    sanitize_columns,
    sanitize_values,
    normalize_string,  # text.py
    setup_logger,  # logging.py
    flatten_children,  # parsers/json.py
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

Uma fonte é **um arquivo** `<fonte>_etl.py` que registra as funções de cada
entidade do YAML:

```python
import logging
from pathlib import Path

from core import Etl, PipelineConfig, run_source, write_bronze

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "camara_config.yml"


def extract_deputados(cfg: PipelineConfig) -> None: ...  # arquivos em cfg.landing_dir
def transform_deputados(cfg: PipelineConfig) -> None:  # termina em write_bronze
    write_bronze(cfg, df)


ETLS = {  # ordem = ordem de execução
    "legislaturas": Etl(transform=transform_legislaturas),  # extract padrão
    "deputados": Etl(extract=extract_deputados, transform=transform_deputados),
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.legislativo.camara.camara_etl [entidade ...] [--steps transform,load]
```

---

## `etl.py`

### `GenericETL(cfg, *, extract_fn=None, transform_fn=None, load_fn=None, log=None)`

- `run(steps=("extract", "transform", "load"))` executa as etapas pedidas, na ordem
  canônica; nome inválido levanta `ValueError`.
- `extract()` / `transform()` / `load()` chamam a função sua ou o default da tabela
  acima. `load_fn` sobrescreve o modo do YAML (uso raro: NHL `player_game_log`).
- Modos de load (o **quê**; o **como** escrever vem de `cfg.write`):
  - `table` — manda o bronze inteiro para um `COPY` em `<db_schema>.<db_table>`
    (`sep=cfg.bronze_sep`), sem pandas e sem chunk. CSV só com cabeçalho cria a
    tabela vazia, sem caso especial.
  - `files` — `PostgresClient.load_files_to_table(cfg.bronze_dir, ...)`: cada CSV
    (`options.file_pattern`, default `*.csv`) vira a tabela de mesmo nome.
  - `jsonb` — `JsonbLoader` sobre `cfg.landing_dir.glob(options.file_pattern or
    landing_file)`, com `options.array_key`, `options.overwrite` e
    `options.control_table` (default `ingestion_control`).
  - `none` — só loga (o orquestrador carrega).
- `db_schema` ausente ou sem prefixo `raw_` levanta `ValueError` antes de conectar.

### `Etl(extract=None, transform=None, load=None)`
Dataclass com as funções de uma entidade; `None` usa o default do `GenericETL`.
Entidades que só diferem por um argumento compartilham a função via
`functools.partial` (ex.: `partial(transform_dados_abertos, url_col="url_votos")`).

### `build_etl(config_file, source, etl) -> GenericETL`
`PipelineConfig.from_yaml(config_file, source)` + `GenericETL` com as funções de
`etl` e logger `<fonte>.<entidade>`. Ponto de entrada para DAGs e testes:
`build_etl(camara_etl.CONFIG_FILE, "votacoes", camara_etl.ETLS["votacoes"]).run(["extract"])`.

### `run_source(config_file, etls: dict[str, Etl], argv=None)`
Entrypoint de todo `<fonte>_etl.py`: `[entidade ...] [--steps a,b]`. Sem entidades,
roda todas na ordem do dict; entidade ou etapa desconhecida sai com erro de uso.
Com mais de uma entidade, isola falhas (loga, segue, `sys.exit(1)` no fim); com uma
só, a exceção propaga. Chama `setup_logger()`.

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
| `load` | de onde carregar: `table` (default) \| `files` \| `jsonb` \| `none` |
| `write` | como escrever na tabela: `truncate` (default) \| `append` \| `merge` |
| `merge_key` | colunas da chave do `merge` (obrigatória com ele, recusada sem ele) |
| `bronze_sep` | separador do bronze (default `;`) |
| `options` | dict livre do bloco `options:` (a core lê só as chaves listadas em `etl.py`) |
| `criar_dirs` | cria os diretórios no `__init__` (default `True`; em testes use `False`) |

Propriedades `landing_filepath`, `bronze_filepath`, `parameter_filepath` levantam
`ValueError` com mensagem clara se o campo não foi configurado.
`write_output_params(df, default_column=None)` exporta os valores únicos de uma
coluna (ou de várias, com o dict) para `parameter_dir`.

Formato do YAML (`db_schema`, `load`, `write`, `bronze_sep` e `options` valem no
topo como default do arquivo e podem ser sobrescritos por source; `options` faz
merge):

```yaml
db_schema: "raw_camara"
load: table                       # opcional
write: truncate                   # opcional (truncate | append | merge)
environments:
  dev:
    base_raw: "${LAKE_ROOT}/raw/demodados/camara"
    base_bronze: "${LAKE_ROOT}/bronze/demodados/camara"
    base_parameters: "${LAKE_ROOT}/raw/demodados/camara/parameters"
  prod:
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
(`validate_raw_schema`).

**A carga é sempre `COPY`, nunca `DROP`.** Um full refresh (`write="truncate"`) é
`CREATE TABLE IF NOT EXISTS` + `TRUNCATE` + `COPY` numa única transação. Três
consequências que valem por si:

- a tabela **nunca é recriada**, então as views do dbt sobre a raw sobrevivem, os
  grants ficam e o OID é estável — é o que torna possível sincronizar prod → dev;
- DDL no Postgres é transacional, então falha no meio do `COPY` faz rollback e a
  tabela **continua com os dados anteriores** (o caminho antigo, em chunks, deixava
  a tabela parcial);
- `TRUNCATE` pega `ACCESS EXCLUSIVE`. A carga usa `SET LOCAL lock_timeout = '30s'`
  para falhar rápido em vez de empilhar fila na frente de um `dbt build`.

**Na raw os dados são sempre texto.** Toda coluna vira `TEXT`; a exceção é a coluna
cujos valores são objetos JSON (dict/list), gravada como `JSONB` (`to_raw_frame`
decide). A tipagem é responsabilidade do dbt (staging). O `JsonbLoader` (NHL) já
grava `payload JSONB`.

**As colunas de rastreio não viajam no stream.** `arquivo_origem` (quando você passa
`filename`) e `data_carga` vêm de `DEFAULT` no catálogo: `data_carga` é o `now()` da
transação, ou seja, **um valor só para a carga inteira, em UTC** — antes era um
`datetime.now()` local por chunk. O `DEFAULT` de `arquivo_origem` só é reescrito
quando muda de valor, porque o `ALTER` pega `ACCESS EXCLUSIVE`.

**NULL vs string vazia no `COPY` (`FORMAT csv`, marcador default `''`):**

- *bronze* (`copy_csv`): campo vazio não aspado é NULL — exatamente o
  `na_values=[""]` com que o pandas lia esses CSVs. `007` continua `007`, `NA` e
  `null` continuam texto. O que o `write_bronze` gera é compatível campo a campo
  (verificado por teste de round-trip em `tests/core/test_copy_load.py`);
- *memória* (`send_df_to_db`): `df_to_csv_buffer` aspa **todo** valor não nulo, então
  `''` sai como `""` (string vazia) e `None` sai como campo vazio (NULL),
  preservando a distinção. De quebra, `;`, `"`, quebra de linha e a linha `\.` ficam
  inofensivos.

Único caso em que o COPY diverge do caminho pandas antigo: uma string vazia
**aspada** (`""`) num bronze produzido fora do `write_bronze` chega como string
vazia, não como NULL.

**Modos de escrita** (`write` no YAML, `write=` nos métodos):

| `write` | O que faz |
|---|---|
| `truncate` (default) | Full refresh: `TRUNCATE` + `COPY`, numa transação |
| `append` | Só `COPY`. Para bronze-delta — ver `control.py`; num bronze completo, duplica |
| `merge` | Upsert: `COPY` para uma temporária + `INSERT ... ON CONFLICT (merge_key) DO UPDATE` |

`merge` exige `merge_key` (uma ou mais colunas, que precisam estar no dado).
`ensure_raw_table` cria o índice único da chave quando não houver — procurando por
**conjunto de colunas**, não por nome, para não duplicar os índices feitos à mão
que openweather e solar já têm. O `INSERT` usa `DISTINCT ON (merge_key)`: sem isso
o `ON CONFLICT` erra com *cannot affect row a second time* quando o lote traz a
mesma chave duas vezes; quando isso acontece, sai um WARNING dizendo quantas
linhas foram descartadas. `arquivo_origem` e `data_carga` também são atualizados
no conflito, então a linha sempre reflete a última carga que a tocou.

**Drift de colunas** (`plan_columns`, função pura): coluna nova no dado vira
`ALTER TABLE ADD COLUMN ... TEXT` com WARNING; coluna que sumiu do dado fica fora do
`COPY` e chega NULL, também com WARNING. Nunca se dropa coluna. Mudança que exige
recreate (rename, `TEXT` ↔ `JSONB`) é gesto manual e logado como WARNING:

```sql
-- derruba a tabela e as views do dbt, que voltam no próximo dbt build
DROP TABLE raw_camara.raw_camara_votacoes CASCADE;
```

Tabela legada criada pelo `to_sql` antigo (com `data_carga` **sem** `DEFAULT`) é
migrada sozinha na primeira carga nova, sem recriação.

| Método | Para quê |
|---|---|
| `copy_csv(path, table_name, *, schema, sep=";", write="truncate", filename=None, merge_key=None, after_copy=None)` | Caminho quente: um CSV inteiro por `COPY`, sem pandas |
| `send_df_to_db(df, table_name, *, schema, write="truncate", filename=None, merge_key=None)` | Grava um DataFrame (tudo `TEXT`, JSON como `JSONB`) |
| `load_files_to_table(input_dir, *, schema, table_name=None, pattern="*.csv", write="truncate", source_column="arquivo_origem", sep=";", merge_key=None)` | Diretório inteiro: com `table_name`, tudo numa tabela; sem, uma tabela por arquivo (stem) |
| `read_sql(sql)` | Resultado como DataFrame (leituras em `staging.*`, `intermediate.*`) |
| `connect()` | Conexão psycopg2 crua (`copy_expert`, transação explícita) |
| `alchemy()` | Engine SQLAlchemy (só leitura) |

---

## `io.py`

```python
write_bronze(cfg, df) -> Path | None
```
**A forma de terminar um transform.** Sanitiza nomes de coluna
(`sanitize_columns`), remove CR/LF dos valores texto (`strip_newlines`), grava
float inteiro como inteiro (`integral_floats_to_int`: coluna inteira com nulo que o
pandas promoveu a float sai `123`, não `123.0`), cria o diretório e grava
`cfg.bronze_filepath` com `cfg.bronze_sep`. DataFrame vazio ou
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
concat_landing(cfg, parse_fn, pattern="*.json") -> DataFrame
```
Aplica `parse_fn(file)` a cada arquivo do landing e concatena (exceção loga e pula o
arquivo; `None`/vazio é ignorado). É o "um DataFrame por arquivo" antes do `write_bronze`.

```python
reset_bronze(cfg) -> None
```
Apaga os CSVs de `cfg.bronze_dir`. Para `load: files`, em que cada CSV vira uma
tabela: sem isso, um CSV antigo viraria tabela fantasma.

```python
list_files(input_dir, pattern="*") -> list[Path]                   # rglob ordenado
concat_files_to_df(input_dir, pattern="*.csv", sep=",", source_column=None) -> DataFrame
write_csv(df, output_dir, filename, sep=";") -> Path               # seeds, exceções
integral_floats_to_int(df) -> DataFrame   # use antes de write_csv num bronze de load: files
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
read_ids(path, column) -> list[str]                              # sem ".0" de float
landing_ids(landing_dir, suffix) -> set[str]
extract_by_ids(cfg, has_data=bool, http=None) -> None
```
O padrão dos "três conjuntos": todos os IDs menos os já no landing menos os que a
API nunca respondeu. `extract_by_ids` monta o loop inteiro a partir do YAML:
placeholder `{id}` em `base_url`/`landing_file`, `parameter_file` com os IDs e, em
`options`, `no_data_file`, `parameter_column` (default `id`) e `blacklist_on_error`
(default `true`: erro/timeout também entra no "sem dados"). `has_data(resposta)`
decide o que é "sem dados" (a câmara usa `dados` não vazio).

---

## `control.py` — carga incremental por arquivo

Uma linha por `(schema, tabela, arquivo)` em `<schema>.<control_table>` diz o que
já entrou. Era exclusivo do `JsonbLoader` (NHL); agora serve também ao caminho
tabular, que é onde estão os volumes grandes do legislativo.

Uma fonte vira incremental por arquivo declarando no YAML, por entidade:

```yaml
write: append
options:
  control_table: "ingestion_control"
```

e trocando o fim do transform por `write_bronze_incremental`:

```python
write_bronze_incremental(cfg, sorted(cfg.landing_dir.glob("*.json")), parse_fn)
```

O ciclo passa a ser:

1. **transform** — pergunta ao controle quais arquivos faltam, gera o bronze só
   com eles e grava um **manifesto** ao lado (`<bronze>.manifesto.csv`) dizendo
   quais foram. Sem pendências, o manifesto sai vazio e não há bronze;
2. **load** — lê o manifesto, faz o `COPY` do bronze-delta e registra aqueles
   arquivos no controle **na mesma transação**. Manifesto vazio: pula a carga.

Falha em qualquer ponto faz rollback, nada é registrado e o transform seguinte
refaz exatamente o mesmo delta. Rodar duas vezes não duplica.

**Nessas entidades o bronze passa a ser o delta da última execução**, não o
histórico completo — quem acumula é a tabela raw. Só vale para landing imutável
(um voto, uma proposição já baixada); o que muda com o tempo (ficha de deputado)
fica em `write: truncate`.

`write_bronze_incremental` sem `options.control_table` é o `write_bronze_streaming`
de sempre, então a mesma função serve aos dois casos e ligar o incremental é uma
mudança de YAML.

**Ao ligar numa tabela que já tem histórico**, rode uma vez o bootstrap — senão o
primeiro delta traz todo o landing e o `append` duplica a tabela:

```bash
uv run python scripts/controle_semear.py <fonte>_config.yml <entidade>
```

| Peça | Para quê |
|---|---|
| `IngestionControl(db, *, schema, table)` | `ensure`/`ingested`/`register`/`clear`/`pending` sobre a tabela de controle |
| `write_bronze_incremental(cfg, files, parse_fn)` | Fim do transform incremental: bronze-delta + manifesto |
| `write_manifest(cfg, files)` / `read_manifest(cfg)` | O manifesto, se você precisar mexer nele |
| `control_for(cfg)` | `IngestionControl` da entidade, ou `None` se o YAML não pediu |

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
entrypoint chama `setup_logger()` — `run_source` já faz isso. Assim o
`INFO` de qualquer módulo chega ao console, sem depender da hierarquia de nomes.

---

## `parsers/`

```python
normalize_json_object(filepath, key=None) -> DataFrame   # pd.json_normalize(sep=".")
flatten_children(records, parent_cols, child_key) -> list[dict]  # uma linha por filho
make_bs_object(input_file=None, response=None) -> BeautifulSoup
```

---

## Convenções ao evoluir esta pasta

- Nada de credencial ou caminho absoluto: use `settings` e `${LAKE_ROOT}` no YAML.
- Um jeito por coisa. Antes de adicionar uma função, veja se é variação de uma que
  existe (parâmetro) ou se serve a uma fonte só (fica no `<fonte>_etl.py`).
- Toda função pública com docstring — é o que aparece nesta referência.
- Cuidado com nomes de topo: `src/` é a raiz de código, então um `src/novo.py` vira
  o módulo global `novo` dentro do venv.
- Testes da lib ficam em `tests/core/`; rode com `uv run task test`.
