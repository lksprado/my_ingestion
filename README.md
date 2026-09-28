# my_ingestion

Extração e carga dos dados pessoais: APIs públicas, scraping e planilhas. Grava no lake (CSV/JSON)
e no Postgres (`raw_<fonte>.<entidade>` no banco do ambiente). Quem agenda é o
[`my_orchestrator`](https://github.com/lksprado/my_orchestrator); a transformação (dbt) fica no
[`my_analytics`](https://github.com/lksprado/my_analytics).

> Visão geral do deploy dos quatro repos (runners, tokens, troubleshooting):
> [`homelab/docs/como_funciona_o_deploy.md`](https://github.com/lksprado/homelab/blob/main/docs/como_funciona_o_deploy.md).

## Conceitos

| Termo | O que é |
|---|---|
| **fonte** | Um sistema de origem (`camara`, `nhl`, `solar`). Uma pasta em `src/pipelines/<domínio>/<fonte>/`, com um `<fonte>_config.yml` e um `<fonte>_etl.py` (as poucas exceções estão marcadas em [Fontes](#fontes)). |
| **entidade** | Uma tabela de uma fonte (`votacoes`, `deputados`). É a mesma palavra no YAML (`sources:`), no dicionário `ETLS` e na linha de comando. |
| **landing** | Arquivos brutos como a origem entregou (JSON, HTML, CSV, PDF), em `${LAKE_ROOT}/raw/...`. É a fonte de verdade: bronze e raw são refeitos a partir dele sem bater na API. |
| **bronze** | O CSV tabular que o transform gera a partir do landing, em `${LAKE_ROOT}/bronze/...`. |
| **raw** | A tabela no Postgres, `raw_<fonte>.<tabela>`, carregada do bronze. Toda coluna é texto (a NHL guarda o JSON em `JSONB`); quem tipa é o dbt. |
| **extract / transform / load** | As três etapas de toda entidade: API → landing → bronze → raw. Cada uma lê e grava disco, e roda sozinha com `--steps`. |
| **ambiente** | `ENV=dev` (sua máquina, banco `ingestion_sandbox`) ou `ENV=prod` (Airflow). Escolhe os caminhos do YAML e o perfil de banco. |

## Fontes

| Domínio | Fonte | O que traz | Destino |
|---|---|---|---|
| clima | [`openweather`](src/pipelines/clima/openweather/README.md) | Resumo meteorológico diário de um ponto | `raw_openweather` |
| energia | [`solar`](src/pipelines/energia/solar/README.md) | Produção do sistema solar de casa (portal APsystems, via Selenium) | `raw_apsystem` |
| esportes | [`nhl`](src/pipelines/esportes/nhl/README.md) | Estatísticas da NHL, JSON bruto em colunas `JSONB` | `raw_nhl` |
| finanças | [`investimentos`](src/pipelines/financas/investimentos/README.md) | Posições da B3 (Excel), Avenue (PDF) e planilhas Google | `raw_b3`, `raw_avenue`, `raw_google_sheets` |
| finanças | [`fundos_imobiliarios`](src/pipelines/financas/fundos_imobiliarios/README.md) | Histórico mensal de FIIs e relatório de dividendos | CSV no lake (exceção: CLI própria, sem banco) |
| legislativo | [`camara`](src/pipelines/legislativo/camara/README.md) | API de Dados Abertos da Câmara | `raw_camara` |
| legislativo | [`senado`](src/pipelines/legislativo/senado/README.md) | API de Dados Abertos do Senado | `raw_senado` |
| legislativo | [`ecidadania`](src/pipelines/legislativo/ecidadania/README.md) | Scraping do e-Cidadania do Senado | `raw_ecidadania` |
| legislativo | [`radar_congresso`](src/pipelines/legislativo/radar_congresso/README.md) | Índice de governismo do Radar Congresso em Foco | `raw_radar_congresso` |
| legislativo | [`ranking_politicos`](src/pipelines/legislativo/ranking_politicos/README.md) | Ranking dos Políticos | `raw_ranking_politicos` |
| legislativo | [`_params`](src/pipelines/legislativo/_params/README.md) | Scripts auxiliares: CSV de deputados atuais e seeds do Senado | parâmetros e seeds do dbt |
| livros | [`vide_editorial`](src/pipelines/livros/vide_editorial/README.md) | Scraping de livros e promoções da Vide Editorial | `raw_vide_editora` |
| preços | [`atacadao`](src/pipelines/precos/atacadao/README.md) | Preços de uma cesta de produtos, para uma inflação pessoal | seed do dbt (exceção: CLI própria, sem banco) |

Nomes de tabela, ordem de execução e armadilhas ficam no README de cada fonte.

## Uso em dev

```bash
uv sync                          # .venv único (Python 3.12)
cp .env.example .env             # preencha; em dev o banco é ingestion_sandbox
uv run pre-commit install        # ruff, gitleaks, validação dos YAMLs e bloqueio de commit na main
```

```bash
uv run task test                 # pytest (sem os de integração; exige DB__DEV__* preenchido)
uv run task lint                 # ruff check + format --check
uv run task format               # ruff format + fix
```

Cada fonte roda sozinha pela linha de comando:

```bash
# uv run python -m pipelines.<domínio>.<fonte>.<fonte>_etl [entidade ...] [--steps extract,transform,load]
uv run python -m pipelines.legislativo.camara.camara_etl                          # todas as entidades
uv run python -m pipelines.legislativo.camara.camara_etl proposicao --steps transform,load
```

### Bancos em dev

As cargas locais vão para o `ingestion_sandbox`, um banco descartável. O `analytics_dev` é do dbt e
só recebe raw por cópia. O único pipeline que lê objeto do dbt é o
`investimentos_fgc` (`int_renda_fixa`), pelo `analytics_dev` (`settings.models_target`).

```bash
scripts/raw_copy.sh seed raw_nhl       # analytics_dev -> sandbox: antes de testar um incremental
scripts/raw_copy.sh promote raw_nhl    # sandbox -> analytics_dev: raw validada, para modelar no dbt
scripts/raw_copy.sh pull raw_nhl       # analytics_prod -> analytics_dev: para a raw de dev não envelhecer
```

A cópia é `TRUNCATE` + `COPY` tabela a tabela, nunca `DROP`: as views do dbt sobre a raw
sobrevivem. Por tabela ele decide sozinho entre completa e só o delta; `--dry-run` mostra a
decisão e `--full` força a completa.

### Onde está cada coisa

- **Pipeline novo:** siga [`src/pipelines/README.md`](src/pipelines/README.md) (onde colocar, YAML, esqueleto, checklist).
- **Biblioteca compartilhada** (`core`): [`src/core/README.md`](src/core/README.md).
- **Tabelas, ordem de execução e armadilhas de uma fonte:** o README da pasta dela (tabela [Fontes](#fontes)).
- **Scripts de manutenção** (cópia de raw, tabela de controle, backfill): [`scripts/README.md`](scripts/README.md).

## Design patterns

Os padrões que dão forma ao código, onde cada um mora e o problema que resolve. As assinaturas
estão em [`src/core/README.md`](src/core/README.md); como aplicá-los num pipeline novo, em
[`src/pipelines/README.md`](src/pipelines/README.md).

### Estrutura do ETL

- **Template Method:** `GenericETL.run()` ([`core/etl.py`](src/core/etl.py)) fixa o esqueleto
  extract → transform → load e a fonte só preenche os passos. Passo ausente cai no default:
  o extract baixa `url_base` e o transform não faz nada.
- **Strategy por injeção de função:** `Etl(extract=..., transform=..., load=...)` recebe funções,
  não subclasses. A variação entre entidades de uma fonte vem de `functools.partial`.
- **Strategy escolhida pela configuração:** `load:` no YAML (`table`, `files`, `jsonb`, `none`)
  seleciona `GenericETL._load_<modo>` por `getattr`; `write:` (`truncate`, `append`) decide
  como gravar. Trocar a forma de carga é mudar uma linha de YAML.
- **Registry:** o dicionário `ETLS` de cada `<fonte>_etl.py`. A chave é o source do YAML e a ordem
  é a de execução. `run_source` (CLI) e as DAGs leem o mesmo registro.
- **Factory:** `build_etl()` e `PipelineConfig.from_yaml()` montam o objeto pronto (ambiente,
  caminhos, logger) a partir do YAML e do registro. Pipeline e DAG não instanciam nada à mão.

### Configuração

- **Configuração declarativa:** o `<fonte>_config.yml` descreve *o quê* (schema, tabela, caminhos,
  modo de carga) e o Python descreve *como*. `PipelineConfig`
  ([`core/config.py`](src/core/config.py)) resolve `${LAKE_ROOT}`, o ambiente (`dev`/`prod` por
  `ENV`) e o placeholder `{date}`; o que a `core` não interpreta vai em `options:`.
- **Injeção de dependência leve:** `PostgresClient()` usa o perfil `DB__<ENV>__*` (no Airflow,
  das variáveis de ambiente); testes injetam um `DbTarget` ou uma `connection`. O logger também
  entra por parâmetro (`log=`).
- **Fail-fast:** `validate_raw_schema` ([`core/db.py`](src/core/db.py)) recusa escrita fora de
  `raw_*`, o validator de [`settings.py`](src/settings.py) exige `ingestion_sandbox` em dev e
  campo obrigatório em `Settings` não tem default. Erro de configuração quebra antes de gravar,
  não no meio da carga.

### Fluxo de dados

- **Etapas com checkpoint em disco (landing → bronze → raw):** cada etapa lê e grava arquivo,
  nada passa em memória. É o que permite rodar só `--steps transform,load`, reexecutar uma
  etapa que falhou e ter cada uma como task separada no Airflow.
- **Pipes and filters entre entidades:** o produtor grava `output_param_file` e o consumidor lê
  `parameter_file`. A dependência entre entidades é um arquivo, e ela define a ordem no `ETLS`.
- **Incremental por diferença de conjuntos:** `pending_ids`, `mark_no_data` e
  `missing_dates_from_landing` ([`core/incremental.py`](src/core/incremental.py))
  calculam *todos − já no landing − marcados sem dados*. Reexecutar não refaz trabalho.
- **Schema-on-read:** a raw é sempre `TEXT` (o JSON bruto da NHL vai como `JSONB`, pelo
  `JsonbLoader`). A tipagem fica no dbt, e mudança de tipo na origem não quebra a carga.

### Carga e resiliência

- **Carga idempotente e atômica:** `TRUNCATE` + `COPY` numa transação, nunca `DROP`. As views do
  dbt sobrevivem e o OID da tabela fica estável. Na carga incremental por arquivo
  ([`core/control.py`](src/core/control.py)), o manifesto é registrado pelo hook `after_copy`
  dentro da mesma transação do `COPY`: ou entram os dados e o registro, ou nenhum dos dois.
- **Null Object no HTTP:** `HttpClient` ([`core/http.py`](src/core/http.py)) devolve `None` em
  vez de levantar exceção. Um ID quebrado é logado e pulado, sem derrubar uma extração longa.
- **Isolamento de falhas:** com várias entidades, `run_source` segue para a próxima quando uma
  falha e termina com `sys.exit(1)` listando as que falharam.

## Deploy

**A `main` é produção.** O código entra no Airflow de prod quando o PR é mergeado.

### Ao abrir o PR

O workflow `CI` (`.github/workflows/ci.yml`) roda em todo PR: `task lint`, o
`scripts/validar_configs.py` e o `task test`. Os testes de integração ficam de fora; rode-os
localmente (`uv run pytest -m integration`) e o pipeline em dev, de preferência pela DAG no
Airflow local, que já lê o seu disco.

Os testes unitários não conectam no banco, mas o `settings.py` valida o perfil `DB__DEV__*` na
importação: sem ele preenchido no `.env` (qualquer valor serve, o banco tem de ser
`ingestion_sandbox`), nem os testes de parser são coletados.

### Ao fazer o merge

1. Se o merge mudou algo em `src/` (fora `.md`), o workflow `Deploy prod`
   (`.github/workflows/deploy-prod.yml`) avisa o `my_orchestrator`. Mudança só em doc, testes ou
   config do repo não dispara.
2. O deploy do `my_orchestrator` publica a ponta dos três repos no atb. Para código deste repo é
   **só rsync, sem restart**: a próxima task já roda com o código novo.
3. O run fica em [my_orchestrator → Actions](https://github.com/lksprado/my_orchestrator/actions).
   Aqui só aparece o aviso: `gh run list -R lksprado/my_ingestion --limit 3`.

### Ordem entre repos

- **Pipeline novo com DAG nova:**
  1. faça o merge daqui **primeiro**;
  2. depois, o da DAG no `my_orchestrator`.

  Na ordem inversa, o deploy da DAG fica vermelho com import error até este merge chegar.
- **Biblioteca Python nova:** o contrário.
  1. Ela entra primeiro no `requirements.txt` do `my_orchestrator`, que reconstrói a imagem.
  2. Depois vem o merge do código daqui que a usa.

## Problemas comuns

| Sintoma | Causa e saída |
|---|---|
| `ENV=dev: faltam DB__DEV__... no .env` ao importar qualquer coisa, inclusive nos testes | O `settings.py` valida o perfil de banco na importação. Preencha o `.env` (qualquer valor passa nos testes unitários, que não conectam). |
| `ENV=dev exige DB__DEV__NAME=ingestion_sandbox` | Em dev a carga só vai para o sandbox, de propósito. Para levar a raw ao `analytics_dev`, use `scripts/raw_copy.sh promote`. |
| `Schema de escrita inválido` | Falta `db_schema` no YAML, ou ele não começa com `raw_`. Toda escrita exige `raw_<fonte>`. |
| `canceling statement due to lock timeout` na carga | Outro processo (quase sempre um `dbt build`) está lendo a tabela. A carga desiste em 30 s em vez de enfileirar; rode de novo quando ele terminar. |
| O commit é barrado pelo `validar-configs-pipeline` | Chave de YAML fora do contrato, ou source sem entrada em `ETLS` (ou o contrário). A mensagem mostra o caminho da chave; ver `validate_config` em [`src/core/README.md`](src/core/README.md). |
| Tabela incremental (`write: append`) com linhas duplicadas | O controle foi ligado numa tabela que já tinha histórico sem o bootstrap. Ver `controle_semear.py` e `controle_reconstruir.py` em [`scripts/README.md`](scripts/README.md). |
| Extract verde, mas a tabela não mudou | O extract de muitas requisições só falha quando **nenhuma** deu certo; falha parcial é WARNING no log. Procure `falharam` no log da etapa. |
