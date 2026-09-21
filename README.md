# my_ingestion

Extração e carga dos dados pessoais: APIs públicas, scraping e planilhas. Grava no lake (CSV/JSON)
e no Postgres (`raw_<fonte>.<entidade>` no banco do ambiente). Quem agenda é o
[`my_orchestrator`](https://github.com/lksprado/my_orchestrator); a transformação (dbt) fica no
[`my_analytics`](https://github.com/lksprado/my_analytics).

> Visão geral do deploy dos quatro repos (runners, tokens, troubleshooting):
> [`homelab/docs/como_funciona_o_deploy.md`](https://github.com/lksprado/homelab/blob/main/docs/como_funciona_o_deploy.md).

## Uso em dev

```bash
uv sync                          # .venv único (Python 3.12)
cp .env.example .env             # preencha; em dev o banco é ingestion_sandbox
uv run pre-commit install        # ruff, gitleaks e bloqueio de commit na main
```

```bash
uv run task test                 # pytest (sem os de integração)
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

- **Pipeline novo:** siga [`src/pipelines/README.md`](src/pipelines/README.md) (onde colocar, YAML, esqueleto, checklist).
- **Biblioteca compartilhada** (`core`): [`src/core/README.md`](src/core/README.md).
- **Tabelas, ordem de execução e armadilhas de uma fonte:** o README da pasta dela.

## Deploy

**A `main` é produção.** O código entra no Airflow de prod quando o PR é mergeado.

### Ao abrir o PR

Não há check no GitHub. A validação é o pre-commit (ruff e gitleaks), o `task test` e rodar o
pipeline em dev, de preferência pela DAG no Airflow local, que já lê o seu disco.

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
