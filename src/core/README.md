# `core/` — biblioteca compartilhada

Tudo que mais de um pipeline precisa mora aqui: cliente HTTP, carga no Postgres,
configuração por YAML, orquestração das três etapas, escrita do bronze e limpeza
de texto. A regra é **um jeito de fazer cada coisa**; helper que serve a uma fonte
só fica no próprio `pipelines/<domínio>/<fonte>/<fonte>_etl.py`.

**Regra prática:** se você está prestes a escrever `requests.Session()`,
`create_engine(...)`, `yaml.safe_load(...)`, `to_csv(...)` de bronze ou
`logging.basicConfig(...)` dentro de um pipeline, já existe aqui.

Tudo que um pipeline usa sai de `from core import ...` (a lista é o `__all__` de
[`__init__.py`](__init__.py)); as seções abaixo descrevem cada módulo.

Parser de JSON vem do submódulo: `from core.parsers.json import normalize_json_object`.
HTML é `BeautifulSoup(html, "html.parser")` direto.

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
CONFIG_FILE = Path(__file__).parent / "<fonte>_config.yml"


def extract_itens(cfg: PipelineConfig) -> None: ...  # arquivos em cfg.landing_dir
def transform_itens(cfg: PipelineConfig) -> None:  # termina em write_bronze
    write_bronze(cfg, df)


ETLS = {  # ordem = ordem de execução
    "resumo": Etl(transform=transform_resumo),  # extract padrão: baixa url_base
    "itens": Etl(extract=extract_itens, transform=transform_itens),
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.<domínio>.<fonte>.<fonte>_etl [entidade ...] [--steps transform,load]
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
- O banco vem de `cfg.db_target`: `ingestion` (default) usa `settings.db_target`;
  `models` usa `settings.models_target` (em dev, o `analytics_dev`; em prod, o mesmo
  banco). Vale também para a tabela de controle.

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
| `output_param_file` | `{arquivo: coluna}` gerado para o próximo pipeline (em `parameter_dir`) |
| `db_table` / `db_schema` | tabela e schema destino (`raw_<fonte>`) |
| `load` | de onde carregar: `table` (default) \| `files` \| `jsonb` \| `none` |
| `write` | como escrever na tabela: `truncate` (default) \| `append` |
| `db_target` | banco da carga: `ingestion` (default) \| `models` (em dev, `analytics_dev`) |
| `bronze_sep` | separador do bronze (default `;`) |
| `options` | dict livre do bloco `options:` (as chaves que a core lê estão na tabela abaixo) |
| `criar_dirs` | cria os diretórios no `__init__` (default `True`; em testes use `False`) |

Propriedades `landing_filepath`, `bronze_filepath`, `parameter_filepath` levantam
`ValueError` com mensagem clara se o campo não foi configurado.
`write_output_params(df)` exporta, para cada `{arquivo: coluna}` de
`output_param_file`, os valores únicos da coluna em `parameter_dir`.

**Chaves de `options` que a `core` lê.** As demais são da fonte e ficam
documentadas no cabeçalho do `<fonte>_config.yml` dela.

| Chave | Quem lê | Default | Para quê |
|---|---|---|---|
| `control_table` | load `table`/`jsonb`, `control.py` | — (`jsonb`: `ingestion_control`) | Tabela de controle da carga incremental por arquivo |
| `file_pattern` | load `files`/`jsonb` | `*.csv` / `landing_file` | Quais arquivos carregar |
| `array_key` | load `jsonb` | — | Chave do JSON com a lista de registros (uma linha por item) |
| `overwrite` | load `jsonb` | `false` | `true`: `TRUNCATE` e recarrega tudo |
| `transform_workers` | `write_bronze_streaming` | `1` | Processos no parse do bronze |
| `workers` | `extract_by_ids` | `1` | Threads HTTP do extract |
| `no_data_file` | `extract_by_ids` | — | CSV dos IDs sem dado (404/410) |
| `parameter_column` | `extract_by_ids` | `id` | Coluna de IDs no `parameter_file` |
| `dias_para_desistir` | `extract_by_ids` | `7` | Dias de erro transitório antes de desistir de um ID |
| `control_file` | `missing_dates_from_landing` | — (obrigatória) | CSV com os dias que faltam |
| `lookback_days` | `missing_dates_from_landing` | `30` | Janela em que um dia sem arquivo é pedido de novo |
| `cutoff_hour` | `missing_dates_from_landing` | `20` | A partir dessa hora, hoje conta como dia completo |

Formato do YAML (`db_schema`, `load`, `write`, `db_target`, `bronze_sep` e `options` valem no
topo como default do arquivo e podem ser sobrescritos por source; `options` faz
merge):

```yaml
db_schema: "raw_camara"
load: table                       # opcional
write: truncate                   # opcional (truncate | append)
db_target: ingestion              # opcional (ingestion | models)
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

### `validate_config(data) -> list[str]`
Valida a estrutura de um `<fonte>_config.yml` já carregado e devolve os erros
(lista vazia = ok), cada um com o caminho da chave (`sources.votos.load: ...`).
Rejeita chave desconhecida no topo, nos ambientes e nas sources (`_source_dict`
ignoraria em silêncio), `load`/`write`/`db_target`/`bronze_sep` inválidos, ambientes que não
sejam exatamente `dev` e `prod`, `base_raw` ausente, path de `dev` fora de
`${LAKE_ROOT}`/`${SEEDS_ROOT}`, `db_schema` sem `raw_` quando `load` é
`table|files|jsonb`, `db_table` ausente com `table|jsonb` e `write: append` com
`load: table` sem `options.control_table` (sem o controle, o bronze é o landing
inteiro e cada carga duplicaria a tabela). Fora isso, `options` é livre.
Roda no pre-commit por `scripts/validar_configs.py`, que também confere as
sources contra as chaves de `ETLS`. Chave nova de YAML entra em
`TOP_KEYS`/`ENV_KEYS`/`SOURCE_KEYS` ou vai em `options:`.

---

## `http.py` — `HttpClient`

`requests.Session` com retry e backoff exponencial (403, 429, 5xx; GET e POST).
Nenhum método levanta exceção de rede: **logam e devolvem `None`**. Sempre teste o
retorno.

```python
HttpClient(log=None, retries=5, backoff_factor=2.0, timeout=30, pool_size=10)
# scraping de site: HttpClient(logger, retries=3, backoff_factor=0.5, timeout=10)
```

| Método | Para quê |
|---|---|
| `request(url, *, method="GET", mode="json", headers=None, data=None, timeout=None, **kw)` | Requisição genérica; `mode` = `json` \| `text` |
| `get_json(url, **kw)` | GET que espera JSON (`params=` para query string) |
| `get_json_status(url, **kw)` | `(dado, status)`: separa 404 (`(None, 404)`) de timeout (`(None, None)`) |
| `get_text(url, **kw)` | GET que devolve HTML/texto, com User-Agent de browser |
| `save_json(data, output_dir, filename)` | Grava JSON indentado (sufixo `.json` garantido); `None` se `data` for `None` |
| `fetch_and_save(url, output_dir, filename)` | `get_json` + `save_json` |
| `fetch_and_save_many(tasks, output_dir, workers=1)` | Lista de `(url, filename)`; threads se `workers > 1`; devolve o número de falhas |

Fora da classe: `ensure_some_success(total, failures, what, log=None)` fecha um
extract com várias requisições (nenhum sucesso levanta, parcial avisa) e
`redact(texto)` mascara parâmetros de credencial numa URL ou mensagem.

`HttpClient(pool_size=10)` é o `pool_maxsize` do adapter: com mais threads que
conexões o urllib3 descarta as excedentes ("Connection pool is full"), então
quem usa `workers > 10` cria o client com `pool_size >= workers`.

---

## `db.py` — `PostgresClient`

A conexão vem de `settings.db_target` (perfil `DB__<ENV>__*` do `.env`, ou das
variáveis de ambiente de mesmo nome — é assim no Airflow, onde o `build_etl` roda
com o perfil `DB__PROD__*`) ou de um `DbTarget` ou `connection` psycopg2 injetado.
Com injeção a `core` nem importa o `settings` (testes).

**Schema é obrigatório em toda escrita** e precisa ser `raw_<fonte>`
(`validate_raw_schema`).

**A carga é sempre `COPY`, nunca `DROP`.** Um full refresh (`write="truncate"`) é
`CREATE TABLE IF NOT EXISTS` + `TRUNCATE` + `COPY` numa única transação. Três
consequências que valem por si:

- a tabela **nunca é recriada**, então as views do dbt sobre a raw sobrevivem, os
  grants ficam e o OID é estável — é o que torna possível sincronizar prod → dev;
- DDL no Postgres é transacional, então falha no meio do `COPY` faz rollback e a
  tabela **continua com os dados anteriores**, nunca parcial;
- `TRUNCATE` pega `ACCESS EXCLUSIVE`. A carga usa `SET LOCAL lock_timeout = '30s'`
  para falhar rápido em vez de empilhar fila na frente de um `dbt build`.

**Na raw os dados são sempre texto.** Toda coluna do bronze vira `TEXT`. A tipagem é responsabilidade do dbt (staging). O `JsonbLoader` (NHL) já
grava `payload JSONB`.

**As colunas de rastreio não viajam no stream.** `arquivo_origem` (quando você passa
`filename`) e `loaded_at_utc` vêm de `DEFAULT` no catálogo: o carimbo é o
`now() AT TIME ZONE 'utc'` da transação, ou seja, **um valor só para a carga
inteira, sempre em UTC** — o `AT TIME ZONE` é o que torna o nome verdade mesmo
num servidor fora de UTC (`now()` puro grava a hora local dele). O `DEFAULT` de
`arquivo_origem` só é reescrito quando muda de valor, porque o `ALTER` pega
`ACCESS EXCLUSIVE`.

Tabela que não tem a coluna (as JSONB da NHL, `raw_apsystem.*`,
`raw_openweather.openweather_daily`) ganha o `ADD COLUMN` na carga seguinte,
por `ensure_loaded_at`: é operação de catálogo, nada é reescrito e as views do
dbt sobrevivem. As linhas que já estavam lá ficam com o instante do `ADD COLUMN`,
não com o da ingestão que as trouxe.

**NULL vs string vazia no `COPY` (`FORMAT csv`, marcador default `''`):** campo
vazio não aspado é NULL — exatamente o `na_values=[""]` com que o pandas lia esses
CSVs. `007` continua `007`, `NA` e `null` continuam texto. O que o `write_bronze`
gera é compatível campo a campo (verificado por teste de round-trip em
`tests/core/test_copy_load.py`).

Ponto de atenção: uma string vazia **aspada** (`""`) num bronze produzido fora
do `write_bronze` chega como string vazia, não como NULL.

**Modos de escrita** (`write` no YAML, `write=` nos métodos):

| `write` | O que faz |
|---|---|
| `truncate` (default) | Full refresh: `TRUNCATE` + `COPY`, numa transação |
| `append` | Só `COPY`. Para bronze-delta — ver `control.py`; num bronze completo, duplica |

Não há upsert: quando a fonte pode reenviar uma linha já carregada, o caminho é
o full refresh a partir do landing (`truncate`), que é o que clima e solar fazem.

**Drift de colunas** (`plan_columns`, função pura): coluna nova no dado vira
`ALTER TABLE ADD COLUMN ... TEXT` com WARNING; coluna que sumiu do dado fica fora do
`COPY` e chega NULL, também com WARNING. Nunca se dropa coluna. Mudança que exige
recreate (rename, `TEXT` ↔ `JSONB`) é gesto manual e logado como WARNING:

```sql
-- derruba a tabela e as views do dbt, que voltam no próximo dbt build
DROP TABLE raw_camara.raw_camara_votacoes CASCADE;
```

Tabela anterior ao carimbo (sem `loaded_at_utc`, ou com ele sem `DEFAULT`) é
reparada sozinha na carga seguinte, sem recriação.

| Método | Para quê |
|---|---|
| `copy_csv(path, table_name, *, schema, sep=";", write="truncate", filename=None, after_copy=None)` | Caminho quente: um CSV inteiro por `COPY`, sem pandas |
| `load_files_to_table(input_dir, *, schema, pattern="*.csv", write="truncate", sep=";")` | `load: files`: cada CSV do diretório vira a tabela de mesmo nome (stem), por `copy_csv` |
| `read_sql(sql_text)` | Resultado como DataFrame (leituras em `staging.*`, `intermediate.*`) |
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
write_bronze_streaming(cfg, files, parse_fn, workers=None) -> Path | None
```
Mesma coisa, um arquivo por vez (memória limitada): `parse_fn(file)` devolve o
DataFrame daquele arquivo (ou `None` para pular). O cabeçalho começa com as
colunas do primeiro e **cresce** quando um arquivo seguinte traz coluna nova: ela
entra no fim e fica vazia (NULL) nas linhas anteriores, e o arquivo só é regravado
nesse caso — nenhuma coluna é descartada. Exceção num arquivo é logada e pulada;
escrita em temporário + `os.replace`. É um **rebuild** completo: quem quiser
incrementalidade filtra `files` antes. `stream_bronze` é a mesma função devolvendo
também quais arquivos foram lidos e quais deram erro (`StreamResult`).

`workers` (default `options.transform_workers` do YAML, senão 1) > 1 faz o parse
e o preparo em paralelo num pool de processos (`forkserver`), com no máximo
`2 * workers` arquivos em andamento; a escrita continua sequencial e na ordem
de `files`, então o bronze sai **idêntico** ao de `workers=1`. Aí `parse_fn`
tem de ser picklável: função de módulo ou `functools.partial` dela, não lambda.
Vale para landings com arquivos grandes (os anuais da Câmara: 62 s → 24 s com 4
workers). `options.workers` é outra coisa: são as threads HTTP do extract.

```python
concat_landing(cfg, parse_fn, pattern="*.json", landing_dir=None) -> DataFrame
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
concat_files_to_df(input_dir, pattern="*.csv", sep=",") -> DataFrame
write_csv(df, output_dir, filename, sep=";") -> Path               # seeds, exceções
integral_floats_to_int(df) -> DataFrame   # use antes de write_csv num bronze de load: files
```

---

## `incremental.py`

Por data (solar, openweather):

```python
missing_dates_from_landing(cfg, now=None) -> list[str]
last_complete_date(cutoff_hour=20, now=None) -> date   # ontem (hoje se >= 20h)
landing_dates(landing_dir, pattern) -> set[date]      # YYYY-MM-DD no nome do arquivo
write_dates_csv(dates, path) / read_dates_csv(path)
```
`missing_dates_from_landing` devolve os dias completos sem JSON no landing
(`landing_file` com `{day}`) e grava a lista em `options.control_file`. "Completo"
é até ontem, ou até hoje depois de `options.cutoff_hour` (20h). Olha os últimos `options.lookback_days`
(default 30) dias — refaz um dia que falhou no meio, que um `MAX(data)` pularia
para sempre — e, sempre, tudo depois do último dia baixado, para uma parada longa
não virar buraco. Não volta para antes do primeiro dia do landing; landing vazio
usa só a janela. Não depende do banco. Para recuperar buraco mais antigo que a
janela, rode o extract uma vez com `lookback_days` maior.

Por ID (legislativo):

```python
pending_ids(all_ids, done_ids, no_data_path=None) -> list[str]   # preserva a ordem
mark_no_data(no_data_path, id_) -> None                          # CSV com header "id"
read_ids(path, column) -> list[str]                              # sem ".0" de float
landing_ids(landing_dir, suffix) -> set[str]
extract_by_ids(cfg, has_data=bool, http=None, today=None) -> None
```
O padrão dos "três conjuntos": todos os IDs menos os já no landing menos os que a
API disse não ter. `extract_by_ids` monta o loop inteiro a partir do YAML:
placeholder `{id}` em `base_url`/`landing_file`, `parameter_file` com os IDs e, em
`options`, `no_data_file`, `parameter_column` (default `id`), `dias_para_desistir`
(default `7`) e `workers` (default `1`; > 1 requisita em threads, com os CSVs de
controle escritos só pela thread principal).

Só resposta definitiva vai para o "sem dados": `has_data(resposta)` falso (a
câmara usa `dados` não vazio) ou 404/410. Erro transitório (timeout, 5xx, 429 depois
dos retries) deixa o ID pendente e anota a data da primeira falha em
`<no_data_file>_erros.csv` (`id,desde`); passado `dias_para_desistir` falhando, o ID
é desistido e vai para o "sem dados" com WARNING. Exceção inesperada (bug nosso)
conta como falha mas não para desistir. No fim, `ensure_some_success`: se nenhum
ID deu certo, levanta `RuntimeError` — contando só os IDs que não vinham falhando
de dias anteriores, para um ID quebrado conhecido não deixar a etapa vermelha até
a desistência (o retry do Airflow no mesmo dia continua vermelho).

---

## `control.py` — carga incremental por arquivo

Uma linha por `(schema, tabela, arquivo)` em `<schema>.<control_table>` diz o que
já entrou. Serve aos dois caminhos: o `JsonbLoader` (NHL) e o tabular, que é
onde estão os volumes grandes do legislativo.

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
   quais foram. Sem pendências, o manifesto sai vazio e não há bronze. Arquivo
   cujo parse levanta exceção fica **fora** do manifesto (ERROR no log) e volta na
   execução seguinte. Se os pendentes lidos não têm nenhuma linha, eles são
   registrados ali mesmo, o bronze velho (o delta anterior, já carregado) é
   apagado e o manifesto sai vazio — senão o load o copiaria de novo;
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

**Quando o que já entrou está errado** (ex.: colunas que o bronze-delta
descartava antes de o cabeçalho crescer), reconstrua a tabela a partir do landing
inteiro. Sem `--confirmar` só simula e mostra as colunas que a tabela ganharia;
com ele, `TRUNCATE` + `COPY` + controle refeito numa transação só:

```bash
uv run python scripts/controle_reconstruir.py <fonte>_config.yml <entidade> [--confirmar]
```

| Peça | Para quê |
|---|---|
| `IngestionControl(db, *, schema, table)` | `ensure`/`ingested`/`register`/`clear`/`pending`/`mark_ingested` sobre a tabela de controle |
| `write_bronze_incremental(cfg, files, parse_fn, log=None)` | Fim do transform incremental: bronze-delta + manifesto |
| `write_manifest(cfg, files)` / `read_manifest(cfg)` | O manifesto, se você precisar mexer nele |
| `control_for(cfg)` | `IngestionControl` da entidade, ou `None` se o YAML não pediu |

---

## `jsonb.py` — `JsonbLoader`

Carga de JSON bruto em tabela `(payload JSONB, source_filename TEXT, loaded_at_utc
TIMESTAMP)` via `COPY`,
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
sanitize_columns(df) -> DataFrame                 # sem acento, minúsculo, só [a-z0-9_]
sanitize_values(df, *, exclude=()) -> DataFrame   # sem acento/pontuação, maiúsculo
strip_newlines(df) -> DataFrame
normalize_string(s) -> str        # "Ações Ordinárias" -> "acoes_ordinarias"
```
Todas devolvem cópia. `strip_newlines` é vetorizado nas colunas `str` (com o
`pyarrow` instalado, strings em Arrow) e vai valor a valor só nas `object`
mistas, onde dict/list ficam intactos. `write_bronze` já aplica `sanitize_columns` e
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
normalize_json_object(filepath, key=None) -> DataFrame   # pd.json_normalize(sep="."); erro sobe
flatten_children(records, parent_cols, child_key) -> list[dict]  # uma linha por filho
```

---

## Convenções ao evoluir esta pasta

- Nada de credencial ou caminho absoluto: use `settings` (ou a `SourceSettings` da
  fonte) e `${LAKE_ROOT}` no YAML.
- Um jeito por coisa. Antes de adicionar uma função, veja se é variação de uma que
  existe (parâmetro) ou se serve a uma fonte só (fica no `<fonte>_etl.py`).
- Toda função pública com docstring — é o que aparece nesta referência.
- Cuidado com nomes de topo: `src/` é a raiz de código, então um `src/novo.py` vira
  o módulo global `novo` dentro do venv.
- Testes da lib ficam em `tests/core/`; rode com `uv run task test`.
