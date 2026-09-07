# Implementando um pipeline novo

Guia de design para adicionar uma fonte ao monorepo. A referência da biblioteca
compartilhada está em [`../core/README.md`](../core/README.md) — leia antes.

O princípio que organiza tudo: **o YAML descreve *o quê* ingerir, o Python descreve
*como*, e a `core` faz o resto.** Se um pipeline novo está escrevendo credencial,
caminho absoluto, `requests.Session()` ou `create_engine()`, ele saiu do trilho.

---

## 1. Onde colocar

```
pipelines/<domínio>/<fonte>/
├── <fonte>_config.yml     # o quê ingerir, por ambiente
├── <fonte>_<entidade>.py  # um script por entidade/tabela
└── README.md              # obrigatório (ver seção 7)
```

O **domínio** é o assunto, não a origem técnica: `legislativo`, `financas`,
`precos`, `energia`, `livros`. Se a fonte nova não cabe em nenhum, crie um domínio
novo — é uma pasta com `__init__.py`. A **fonte** é o sistema de origem (`camara`,
`atacadao`, `vide_editorial`), não a entidade.

Convenções de nome, na cascata:

| Item | Padrão | Exemplo |
|---|---|---|
| Pasta | `<fonte>` | `camara/` |
| Script | `<fonte>_<entidade>.py` | `camara_votos_deputados.py` |
| Source no YAML | `<entidade>` | `votos_deputados` |
| Tabela | `raw_<fonte>_<entidade>` | `raw_camara_votos_deputados` |

Um script por tabela destino. Resista à tentação de fazer um script que carrega
cinco tabelas: a granularidade é o que permite reexecutar só o que falhou.

---

## 2. Escreva o YAML primeiro

Ele força as decisões antes do código e é o que mantém o pipeline livre de caminhos
hardcoded:

```yaml
environments:
  local:
    base_raw: "${LAKE_ROOT}/raw/<fonte>"
    base_bronze: "${LAKE_ROOT}/bronze/<fonte>"
    base_parameters: "${LAKE_ROOT}/raw/<fonte>/parameters"
  airflow:
    base_raw: "/usr/local/airflow/mylake/raw/<fonte>"
    base_bronze: "/usr/local/airflow/mylake/bronze/<fonte>"
    base_parameters: "/usr/local/airflow/mylake/raw/<fonte>/parameters"

sources:
  <entidade>:
    base_url: "https://api.exemplo/v1/<entidade>"
    subpath: "<entidade>"              # separa esta entidade dentro do raw/bronze
    bronze_file: "<fonte>_<entidade>.csv"
    db_table: "raw_<fonte>_<entidade>"
```

Regras:

- **Sempre `${LAKE_ROOT}`/`${SEEDS_ROOT}`**, nunca `/media/...` ou `/home/...`.
- Os dois ambientes (`local` e `airflow`) existem porque o mesmo código roda na
  máquina e no orquestrador. O ativo vem de `ENV` no `.env`.
- `subpath` evita que entidades da mesma fonte se misturem no mesmo diretório.
- `landing_file` e `bronze_file` aceitam `{date}`, substituído pela data de hoje.

---

## 3. O esqueleto do script

```python
import logging
from pathlib import Path

import pandas as pd

from core import GenericETL, PipelineConfig, load_source_config
from core.http import HttpClient
from core.text import ColumnSanitizer

logger = logging.getLogger("raw_<fonte>_<entidade>")

_CONFIG_FILE = Path(__file__).parent / "<fonte>_config.yml"


def extract(cfg: PipelineConfig):
    """Busca o dado bruto e grava em cfg.landing_dir."""
    extractor = HttpClient(logger)
    extractor.fetch_and_save(
        url=cfg.url_base,
        output_dir=cfg.landing_dir,
        filename="<entidade>.json",
    )


def transform(cfg: PipelineConfig):
    """Lê o landing, normaliza e grava o CSV em cfg.bronze_filepath."""
    dataframes = []
    for json_file in cfg.landing_dir.glob("*.json"):
        df = pd.read_json(json_file)
        df = ColumnSanitizer(df).sanitize_columns_names().df
        dataframes.append(df)

    dfs = pd.concat(dataframes, ignore_index=True)
    dfs.to_csv(cfg.bronze_filepath, sep=";", index=False)


def run_pipeline(cfg: PipelineConfig) -> None:
    GenericETL(
        cfg=cfg, extract_fn=extract, transform_fn=transform, load_fn=None, log=logger
    ).run()


if __name__ == "__main__":
    config = load_source_config(_CONFIG_FILE, source="<entidade>")
    run_pipeline(PipelineConfig(**config))
    # uv run python -m pipelines.<domínio>.<fonte>.<fonte>_<entidade>
```

Sempre deixe o comentário com o comando de execução no fim do `__main__` — é o que
as pessoas copiam.

### Quais etapas você realmente escreve

| Etapa | Default da `core` | Quando escrever a sua |
|---|---|---|
| `extract` | Baixa `cfg.url_base` para `cfg.landing_filepath` | Paginação, parametrização por IDs, Selenium, POST/GraphQL — quase sempre |
| `transform` | **Não tem default** | Sempre. É o que difere uma fonte da outra |
| `load` | Lê o bronze (`;`) e grava em `raw.<db_table>` | Raro: só se o destino não for uma tabela (CSV, seed do dbt) |

Passar `load_fn=None` é o caso comum e significa "use o loader padrão" — não
significa "não carregue".

---

## 4. Padrões que já existem — reaproveite

**Bronze em CSV `;`.** O loader padrão lê com `sep=";"`. Se gravar com `,`, a carga
quebra.

**Nomes de coluna sempre sanitizados** com `ColumnSanitizer(df).sanitize_columns_names().df`.
Para preservar o case de URLs e e-mails nos *valores*, use
`not_sanitize_columns_values([...])`.

**Extração parametrizada por IDs.** Quando uma entidade depende dos IDs de outra, o
produtor declara `output_param_file` e o consumidor declara `parameter_file`:

```yaml
votacoes:
  output_param_file:                 # {arquivo: coluna}
    id_votacoes.csv: id
    id_proposicao.csv: id_proposicao

votos_deputados:
  parameter_file: id_votacoes.csv    # consome o que votacoes gerou
```

No transform do produtor: `cfg.write_output_params(df, logger=logger)`.
Documente a ordem de execução resultante no README da fonte.

**Extração incremental.** Para fontes com milhares de IDs, o padrão adotado é
comparar três conjuntos e requisitar só a diferença:

1. todos os IDs (do `parameter_file`),
2. os já baixados (arquivos existentes no `landing_dir`),
3. os que a API não respondeu antes (um CSV de "sem dados" no `parameter_dir`).

Registrar o terceiro conjunto é o que evita martelar a API com IDs que nunca
voltam. Veja `legislativo/camara/camara_votos_deputados.py`.

**Falhas não abortam o lote.** O `HttpClient` devolve `None` em erro em vez de
levantar exceção — trate o item, logue e siga. Um ID quebrado não pode derrubar uma
extração de 4 horas.

---

## 5. Quando *não* usar o `GenericETL`

O `GenericETL` pressupõe o fluxo landing → bronze → tabela. Nem toda fonte é assim,
e forçar o encaixe piora o código. Precedentes no repositório:

- **`precos/atacadao`** — a saída é CSV para virar seed do dbt; não há carga em
  banco. Usa `HttpClient` e `core.io` direto, com um `run.py` próprio.
- **`financas/fundos_imobiliarios`** — CLI com `--month/--force/--consolidate-only`,
  consolidação idempotente e relatório HTML. Fluxo mensal, não ETL linear.
- **`financas/investimentos`** — quatro fontes heterogêneas (Excel, PDF, Sheets, DW)
  com um orquestrador `run_all.py` que isola falhas por fonte.

O critério: **use o `GenericETL` quando o fluxo for landing → bronze → `raw.*`.**
Fora disso, monte o seu, mas continue usando `HttpClient`, `PostgresClient`,
`core.io` e `setup_logger` — a padronização que importa é a da biblioteca, não a do
orquestrador.

---

## 6. Configuração e segredos

- Credencial nova vai para o `.env` **e** para o `.env.example` (com valor vazio), e
  vira campo em `src/settings.py`. Nunca leia `os.getenv` espalhado pelo código.
- Campo obrigatório em `Settings` não leva default: é melhor falhar na importação do
  que criar diretórios errados com valor vazio.
- Arquivo de credencial (chave de service account, por exemplo) mora **fora do
  repo**, em `~/.secrets/`, e o `.env` guarda só o caminho.

---

## 7. Antes de considerar pronto

- [ ] `README.md` na pasta da fonte: o que coleta (tabela script → source → tabela
      destino), dependências entre pipelines, como executar, e as armadilhas
      conhecidas (seletor frágil, binário externo, limite de paginação).
- [ ] Testes em `tests/<domínio>/`. Priorize os **parsers**, que é onde a regressão
      silenciosa mora. Em teste, `PipelineConfig(..., criar_dirs=False)`.
- [ ] `uv run task lint` e `uv run task test` limpos.
- [ ] O módulo importa isolado:
      `uv run python -c "import pipelines.<domínio>.<fonte>.<script>"`.
- [ ] Rodou de verdade uma vez e conferiu a tabela em `raw.*`.
- [ ] Nenhum caminho absoluto e nenhum segredo no diff (o `pre-commit` roda o
      gitleaks, mas confira).

---

## 8. Dívidas conhecidas do código migrado

Ao copiar um pipeline existente como modelo, saiba o que **não** imitar:

- Os pipelines do `legislativo` usam `logging.getLogger(...)` no módulo e
  `logging.basicConfig(...)` no `__main__`, herdados da migração. Em código novo,
  prefira `setup_logger(__name__)` da `core`.
- Alguns passam `env="local"` explicitamente no `load_source_config`. Omita: o
  default vem de `settings.env`, e é isso que faz o mesmo código rodar no Airflow.
- `_params/dbt_seed_maker.py` grava em caminho absoluto para o repo do dbt. É
  exceção consciente (aquele warehouse não é o `SEEDS_ROOT`), não um exemplo.
