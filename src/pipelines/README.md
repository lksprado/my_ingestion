# Implementando um pipeline novo

Guia de design para adicionar uma fonte ao monorepo. A referência da biblioteca
compartilhada está em [`../core/README.md`](../core/README.md) — leia antes.

O princípio que organiza tudo: **o YAML descreve *o quê* ingerir, o Python descreve
*como*, e a `core` faz o resto.** Todo pipeline é **extract → transform → load**:
arquivos brutos no landing, CSV no bronze, tabela em `raw_<fonte>`. Se um pipeline
novo está escrevendo credencial, caminho absoluto, `requests.Session()`,
`create_engine()` ou `to_csv` de bronze, ele saiu do trilho.

---

## 1. Onde colocar

```
pipelines/<domínio>/<fonte>/
├── <fonte>_config.yml     # o quê ingerir, por ambiente (uma source por entidade)
├── <fonte>_etl.py         # o ETL de todas as entidades da fonte
└── README.md              # obrigatório (ver seção 7)
```

O **domínio** é o assunto, não a origem técnica: `legislativo`, `financas`,
`precos`, `energia`, `clima`, `esportes`, `livros`. A **fonte** é o sistema de
origem (`camara`, `atacadao`, `vide_editorial`), não a entidade.

Convenções de nome, na cascata:

| Item | Padrão | Exemplo |
|---|---|---|
| Pasta | `<fonte>` | `camara/` |
| Script | `<fonte>_etl.py` | `camara_etl.py` |
| Source no YAML e chave de `ETLS` | `<entidade>` | `votos_deputados` |
| Schema | `raw_<fonte>` (chave `db_schema` no topo do YAML) | `raw_camara` |
| Tabela | `<entidade>` | `raw_camara.votos_deputados` |

Schema e tabela são o padrão para fonte **nova**. Se o `my_analytics` já lê uma tabela
equivalente (ver `models/staging/_sources.yml`), aponte `db_schema`/`db_table` para ela
(ex.: `raw_camara.raw_camara_votos_deputados`) em vez de criar outra e mudar o source do dbt.

**Um script de ETL por fonte.** As entidades de uma mesma origem compartilham
cliente, envelope de resposta e parsers; espalhá-las em um script por tabela
duplica o esqueleto e empurra o que é comum para arquivos `_common.py`. Parsers e
helpers da fonte moram no próprio `<fonte>_etl.py`; o que serve a mais de uma fonte
vai para a `core`. A granularidade para reexecutar só o que falhou vem da CLI
(`<fonte>_etl votacoes --steps transform`), não do arquivo.

**Separe em mais de um módulo só quando a estrutura muda muito** entre as entidades
(formato de origem, bibliotecas, centenas de linhas de parser sem nada em comum).
Mesmo aí há um único entrypoint `<fonte>_etl.py` que registra as funções dos
módulos: é o caso de `financas/investimentos` (Excel da B3, PDF da Avenue, Google
Sheets em `investimentos_b3.py`/`_avenue.py`/`_google.py`).

---

## 2. Escreva o YAML primeiro

Ele força as decisões antes do código e é o que mantém o pipeline livre de caminhos
hardcoded:

```yaml
db_schema: "raw_<fonte>"               # schema de todas as tabelas deste arquivo
load: table                            # de onde: table (default) | files | jsonb | none
write: truncate                        # como: truncate (default) | append

environments:
  dev:                                 # execução local
    base_raw: "${LAKE_ROOT}/raw/<fonte>"
    base_bronze: "${LAKE_ROOT}/bronze/<fonte>"
    base_parameters: "${LAKE_ROOT}/raw/<fonte>/parameters"
  prod:                                # Airflow
    base_raw: "/usr/local/airflow/mylake/raw/<fonte>"
    base_bronze: "/usr/local/airflow/mylake/bronze/<fonte>"
    base_parameters: "/usr/local/airflow/mylake/raw/<fonte>/parameters"

sources:
  <entidade>:
    base_url: "https://api.exemplo/v1/<entidade>"
    subpath: "<entidade>"              # separa esta entidade dentro do raw/bronze
    landing_file: "<entidade>_{date}.json"
    bronze_file: "<fonte>_<entidade>.csv"
    db_table: "<entidade>"             # -> raw_<fonte>.<entidade>
```

Regras:

- **Sempre `${LAKE_ROOT}`/`${SEEDS_ROOT}`**, nunca `/media/...` ou `/home/...`.
- São só dois ambientes: `dev` (execução local) e `prod` (Airflow), porque o mesmo código roda na
  máquina e no orquestrador. O ativo vem de `ENV` no `.env`.
- `subpath` evita que entidades da mesma fonte se misturem no mesmo diretório.
- `landing_file` e `bronze_file` aceitam `{date}`, substituído pela data de hoje.
  É o **único** placeholder que a core resolve, e só nesses dois campos; os
  outros (`{id}`, `{day}`, `{game_id}`, e qualquer um em `base_url`) ficam para
  o script resolver com `str.format`.
- `db_schema`, `load`, `write`, `bronze_sep` e `options` valem no topo (default do
  arquivo) e por source (override).
- `load` diz **de onde** carregar, `write` diz **como** escrever na tabela. O
  default `truncate` é full refresh por `TRUNCATE` + `COPY` numa transação — a
  tabela nunca é recriada, então as views do dbt sobre a raw sobrevivem.
- `write: append` + `options.control_table` faz carga incremental por arquivo,
  para landing imutável com milhares de arquivos (legislativo por ID): o transform
  termina em `write_bronze_incremental` e só o que ainda não entrou vai para o
  bronze. Ver `control.py` no `core/README.md` — inclusive o bootstrap obrigatório
  (`scripts/controle_semear.py`) ao ligar numa tabela que já tem histórico.
  `append` sobre um bronze completo duplica a tabela.
- Chaves que só a sua fonte entende vão num bloco `options:` e chegam em
  `cfg.options` como dict. Documente-as no cabeçalho do YAML.

---

## 3. O esqueleto do script

```python
"""ETL da <fonte> -> ``raw_<fonte>.<entidade>``."""

import logging
from functools import partial
from pathlib import Path

import pandas as pd

from core import (
    Etl,
    HttpClient,
    PipelineConfig,
    concat_landing,
    run_source,
    write_bronze,
)

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "<fonte>_config.yml"


def extract_paginado(cfg: PipelineConfig) -> None:
    """Busca o dado bruto e grava em cfg.landing_dir."""
    ...


def parse_envelope(path: Path, key: str) -> pd.DataFrame | None:
    """Um arquivo do landing -> DataFrame (função pura: é o que os testes cobrem)."""
    ...


def transform(cfg: PipelineConfig, key: str) -> None:
    """Lê o landing, normaliza e grava o bronze."""
    write_bronze(cfg, concat_landing(cfg, partial(parse_envelope, key=key)))


ETLS = {  # ordem = ordem de execução (produtores de parâmetros primeiro)
    "<entidade_a>": Etl(transform=partial(transform, key="dados")),  # extract padrão
    "<entidade_b>": Etl(
        extract=extract_paginado, transform=partial(transform, key="items")
    ),
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.<domínio>.<fonte>.<fonte>_etl [entidade ...] [--steps transform,load]
```

Cada chave de `ETLS` é uma source do YAML. Entidades que só diferem por um argumento
compartilham a função com `functools.partial`, sem copiar código. Sempre deixe o
comentário com o comando de execução no fim do `__main__`: é o que as pessoas
copiam. Sem entidades, `run_source` roda todas na ordem do dict (falha de uma não
aborta as outras); `--steps` roda um subconjunto das etapas (é assim que o
orquestrador as separa em tasks, e como você reprocessa sem bater na API). Em DAG
ou teste, `build_etl(CONFIG_FILE, "<entidade>", ETLS["<entidade>"]).run([...])`.

### Quais etapas você realmente escreve

| Etapa | Default da `core` | Quando escrever a sua |
|---|---|---|
| `extract` | Baixa `cfg.url_base` para `cfg.landing_filepath`; sem `url_base`, no-op | Paginação, parametrização por IDs, Selenium, POST — quase sempre |
| `transform` | No-op | Sempre que houver bronze (é o que difere uma fonte da outra); termina em `write_bronze` |
| `load` | Por `load:` do YAML — `table`, `files`, `jsonb`, `none` | Praticamente nunca (`load_fn` só para casos como carregar uma subpasta) |

---

## 4. Padrões que já existem — reaproveite

**Bronze sempre por `write_bronze(cfg, df)`** (ou `write_bronze_streaming` quando o
landing tem milhares de arquivos). Ele sanitiza nomes de coluna, remove quebras de
linha dos valores, grava inteiros sem `.0`, grava com `cfg.bronze_sep` (`;`) e
preserva o bronze anterior se não houver dado. Não chame `to_csv` direto.

**A raw é texto.** Todo load tabular grava as colunas como `TEXT`, por `COPY` do
bronze CSV; não tipe nada no transform pensando no banco, e deixe o
cast para o staging do dbt. Com `load: files` e `write_csv` próprio, passe o
DataFrame por `integral_floats_to_int` antes (ver `investimentos_b3`).

**Nomes de coluna** são definidos por `sanitize_columns` (dentro do `write_bronze`).
Chame explicitamente só quando a lógica do transform depende do nome sanitizado.
Para normalizar valores preservando URLs e e-mails, `sanitize_values(df, exclude=[...])`.

**Extração parametrizada por IDs.** Quando uma entidade depende dos IDs de outra, o
produtor declara `output_param_file` e o consumidor declara `parameter_file`:

```yaml
votacoes:
  output_param_file:                 # {arquivo: coluna}
    id_votacoes.csv: id
    id_proposicao.csv: id_proposicao

votos_deputados:
  base_url: ".../votacoes/{id}/votos"
  landing_file: "{id}_votos_deputados.json"
  parameter_file: id_votacoes.csv    # consome o que votacoes gerou
  options:
    no_data_file: sem_dados_id_votacao.csv
```

No transform do produtor: `cfg.write_output_params(df, default_column="id")`.
Documente a ordem de execução resultante no README da fonte.

**Extração incremental por ID.** Para fontes com milhares de IDs, o padrão é
comparar três conjuntos e requisitar só a diferença: todos os IDs
(`parameter_file`), os já no `landing_dir`, e os que a API não respondeu
(`options.no_data_file`). `core.pending_ids`/`mark_no_data` fazem a conta;
`core.extract_by_ids(cfg, has_data=...)` monta o loop inteiro a partir do YAML
(placeholder `{id}` em `base_url`/`landing_file`). `options.workers` (default 1)
faz as requisições em threads; mantenha baixo (4) para não cair em 429 da API.

**Extração incremental por data.** `core.missing_dates_from_db(db, sqls, control)`
descobre no Postgres até onde os dados vão e devolve as datas faltantes. Veja
`clima/openweather` e `energia/solar`.

**Falhas não abortam o lote.** O `HttpClient` devolve `None` em erro em vez de
levantar exceção — trate o item, logue e siga. Um ID quebrado não pode derrubar uma
extração de 4 horas.

**Várias entidades de uma vez.** `<fonte>_etl a b c` roda só essas, em sequência,
isolando falhas (ex.: os seis dinâmicos da NHL depois do `games_summary`).

---

## 5. Exceções ao `GenericETL`

O critério é simples: **se não escreve em `raw_<fonte>.*`, não é um pipeline de
ingestão** e não precisa do `GenericETL`. Hoje são:

- **`precos/atacadao`** — coleta CSV no landing e consolida um seed do dbt.
- **`financas/fundos_imobiliarios`** — CLI mensal com histórico consolidado em CSV e
  relatório HTML (`dividend_report.py`).
- **`financas/investimentos/investimentos_fgc.py`** — lê `intermediate.*` do DW e
  grava um seed.

Mesmo aí, use `HttpClient`, `PostgresClient.read_sql`, `write_csv`,
`concat_files_to_df` e `setup_logger()` da `core`. Tudo o mais — inclusive fontes
sem transform (NHL, `load: jsonb`) e sem load (solar/openweather, `load: none`) —
está no padrão.

---

## 6. Configuração e segredos

- Credencial nova vai para o `.env` **e** para o `.env.example` (com valor vazio), e
  vira campo em `src/settings.py`. Nunca leia `os.getenv` espalhado pelo código.
  Grupos de valores usam o delimitador duplo (`URL_FINANCE__<CHAVE>` →
  `settings.url_finance[<chave>]`).
- Campo obrigatório em `Settings` não leva default: é melhor falhar na importação do
  que criar diretórios errados com valor vazio.
- Arquivo de credencial (chave de service account, por exemplo) mora **fora do
  repo**, em `~/.secrets/`, e o `.env` guarda só o caminho.
- Banco: nunca escolha o destino no código. `PostgresClient()` usa o perfil
  `DB__<ENV>__*` do ambiente ativo (`settings.db_target`); em `dev` é sempre
  `ingestion_sandbox`. Leitura de objeto do dbt (raro) usa `settings.models_target`. O schema vem do YAML (`db_schema: raw_<fonte>`) e toda escrita
  exige `schema=` explícito — a `core` recusa qualquer coisa sem o prefixo `raw_`.

---

## 7. Antes de considerar pronto

- [ ] `README.md` na pasta da fonte: o que coleta (tabela script → source → tabela
      destino), dependências entre pipelines, como executar, e as armadilhas
      conhecidas (seletor frágil, binário externo, limite de paginação).
- [ ] Testes em `tests/<domínio>/`. Priorize os **parsers**, que é onde a regressão
      silenciosa mora. Em teste, `PipelineConfig(..., criar_dirs=False)` ou
      `tmp_path`.
- [ ] `uv run task lint` e `uv run task test` limpos.
- [ ] O módulo importa isolado:
      `uv run python -c "import pipelines.<domínio>.<fonte>.<fonte>_etl"`.
- [ ] Rodou de verdade uma vez e conferiu a tabela em `raw_<fonte>.*` do
      `ingestion_sandbox`: contagem esperada, `loaded_at_utc` não nula e
      `count(DISTINCT loaded_at_utc) = 1` (uma carga, um timestamp).
- [ ] Nenhum caminho absoluto e nenhum segredo no diff (o `pre-commit` roda o
      gitleaks, mas confira).

---

## 8. Dívidas conhecidas

- `legislativo/ecidadania` guarda no landing o CSV já parseado, não o HTML bruto.
- Comportamentos registrados por fonte (timeout na lista "sem dados", vínculo
  senado_status ← ecidadania) estão nos READMEs de `camara` e `senado`.
