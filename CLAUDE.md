# CLAUDE.md

Monorepo de ingestão: APIs, scraping e planilhas → lake local (`${LAKE_ROOT}`) e Postgres (`raw_<fonte>`).
dbt fica em `~/workspace/my_analytics`; Airflow em `~/workspace/my_orchestrator`. Código, comentários,
docs e commits em **português**.

Leia antes de mexer no que cobrem: `src/core/README.md` (API da core, `options`, incremental, controle),
`src/pipelines/README.md` (pipeline novo + checklist de pronto), `src/pipelines/<domínio>/<fonte>/README.md`,
cabeçalho de `scripts/raw_copy.sh`.

## Comandos

```bash
uv sync && cp .env.example .env && uv run pre-commit install   # .env é obrigatório para importar settings
uv run task test          # unit (-m 'not integration'); uv run pytest -m integration para Postgres/rede
uv run task lint          # ou: uv run task format
uv run python -m pipelines.<domínio>.<fonte>.<fonte>_etl [entidade ...] [--steps transform,load]
```

## Regras

- **Imports planos**: `src/` é a raiz — `from core import ...`, `from settings import settings`,
  `from pipelines.x.y.y_etl import ...`. Nunca `src.` nem `my_ingestion.`.
- **Uma pasta e um `<fonte>_etl.py` por fonte**, parsers inclusos; nada de `_common.py`/`_parsers.py`
  (o que serve a várias fontes vai para `core`). Define `ETLS = {"<entidade>": Etl(...)}` (chave = source
  do YAML, ordem = execução) e termina em `run_source(CONFIG_FILE, ETLS)`. DAGs/testes usam `build_etl`.
- **extract → transform → load, cada etapa lê e grava disco** (são tasks separadas no Airflow).
  Bronze sempre via `write_bronze`/`write_bronze_streaming` (exceção: `load: files`).
- **YAML `<fonte>_config.yml`**: `db_schema` no topo, `environments: {dev, prod}` apenas; `dev` usa
  `${LAKE_ROOT}`/`${SEEDS_ROOT}`, `prod` usa `/usr/local/airflow/mylake/...`. Chaves não interpretadas pela
  core vão em `options:`. Não passe `env=` ao `PipelineConfig`.
- **Carga**: sempre `COPY` numa transação, `truncate` ou `append`, **nunca `DROP`** (views do dbt dependem).
  Raw é sempre `TEXT` (exceção: `JsonbLoader`); tipagem é do dbt. `arquivo_origem`/`loaded_at_utc` são
  `DEFAULT` do catálogo.
- **Banco**: `ENV` escolhe o perfil `DB__<ENV>__*`; `dev` é obrigatoriamente `ingestion_sandbox`.
  `PostgresClient()` sem argumentos; nunca `db_name=`. Escrita exige `schema=` explícito `raw_*`.
  Leitura de modelos dbt via `PostgresClient(settings.models_target)`.
- **`HttpClient` devolve `None` em erro**: trate, logue e siga; extract termina com
  `ensure_some_success(total, falhas)`.
- **Nomes legados — não renomeie** (o `my_analytics` lê em `models/staging/_sources.yml`):
  `raw_camara.raw_camara_*` (`votos_orientacao` → `raw_camara_votacoes_orientacao`), `raw_senado.raw_senado_*`,
  `raw_ecidadania.raw_ecidadania_*`, `raw_radar_congresso.raw_radar_*`, `raw_ranking_politicos.raw_ranking_*`,
  `raw_google_sheets.*`, `raw_vide_editora.vide_raw_home_featured`,
  `raw_apsystem.solar_daily_energy|solar_hourly_energy`, `raw_openweather.openweather_daily`,
  `raw_nhl.nhl_raw_*`, `raw_nhl.nhl_ingestion_control`; pastas `raw/nhl/*`, `raw/demodados/*`, `raw/vide`.
  Fonte nova: `raw_<fonte>.<entidade>`.

## Convenções

- Credencial nova: `.env` + `.env.example` (vazio) + campo em `settings.py` (obrigatório sem default)
  + `.env.example` do `my_orchestrator` + `gh secret set <NOME> --env prod -R lksprado/my_orchestrator`
  (o `.env` de prod é gerado no deploy; arquivo vai como secret `ARQUIVO__<NOME>_<EXT>`).
  Nunca `os.getenv`. Arquivos de credencial em `~/.secrets/`, `.env` guarda o caminho.
- Nunca caminho absoluto em código (`settings.lake_root`/`seeds_root`) nem no bloco `dev` do YAML.
- Logger: `logging.getLogger(__name__)`; só o entrypoint configura (o `run_source` já faz). Nunca `basicConfig`.
- Testes em `tests/<domínio>/`, priorizando parsers; `@pytest.mark.integration` para Postgres/rede.
- `ruff` (`E,F,I,N,W,UP,B`, linha 88). Pipeline novo só está pronto com README na pasta da fonte.
- Commits: `feat:` `fix:` `doc:` `test:` `build:` `refactor:` `chore:` `remove:`.
