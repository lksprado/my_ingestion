# Pipeline: NHL

Extrai estatísticas de hóquei das APIs públicas da NHL (`api-web.nhle.com` e
`api.nhle.com/stats`) e carrega os JSONs **sem transformação** em tabelas JSONB
(`payload`, `source_filename`) no schema `raw_nhl` (`load: jsonb` no YAML). A
normalização acontece no dbt [`my_datawarehouse`](https://github.com/lksprado/my_datawarehouse)
(seletor `nhl`).

Migrado do repo `nhl-extraction` (submódulo `include/nhl_extraction` do airflow3).

## O que coleta

| Script | Source | Tabela | Carga |
|---|---|---|---|
| `nhl_seasons.py` | `seasons` | `raw_nhl.nhl_raw_all_seasons_id` | full (anual) |
| `nhl_teams.py` | `teams` | `raw_nhl.nhl_raw_all_teams_id` | full (anual) |
| `nhl_games_summary.py` | `games_summary` | `raw_nhl.nhl_raw_all_games_summary` | full (diário) |
| `nhl_games_details.py` | `games_details` | `raw_nhl.nhl_raw_all_games_details` | incremental |
| `nhl_games_summary_details.py` | `games_summary_details` | `raw_nhl.nhl_raw_all_games_summary_details` | incremental |
| `nhl_play_by_play.py` | `play_by_play` | `raw_nhl.nhl_raw_all_play_by_play` | incremental |
| `nhl_club_stats.py` | `club_stats` | `raw_nhl.nhl_raw_all_club_stats` | full |
| `nhl_players.py` | `players` | `raw_nhl.nhl_raw_all_players` | full |
| `nhl_player_game_log.py` | `player_game_log` | `raw_nhl.nhl_raw_all_player_game_log` | full (temporada mais recente) |

Os nomes de tabela e de pasta (`raw/nhl/single`, `raw/nhl/raw_all_games_details`…)
**fogem do padrão** `raw_<fonte>.<entidade>` do monorepo de propósito: o dbt os
referencia em `_sources.yml` e o lake já tem ~170 mil JSONs nesses diretórios. Só
o schema segue o padrão (`raw_nhl`, chave `db_schema` do YAML).

## Como funciona

`_common.build(source)` monta o `GenericETL` a partir do YAML: sources **estáticos**
usam o extract padrão da `core` (uma requisição a `base_url`); sources **dinâmicos**
(com `param_view`) usam `extract_dynamic`, que lê os IDs de uma view do dbt e
requisita `base_url.format(**linha)`. Não há transform. O load é o modo `jsonb` da
`core` (`JsonbLoader`, controle em `raw_nhl.nhl_ingestion_control`); só
`player_game_log` usa um `load_fn` próprio para carregar a temporada mais recente.

## Dependências entre pipelines (ordem de execução)

Os seis pipelines dinâmicos descobrem **quais IDs requisitar** em views do dbt
(`staging.vw_stg_request_*`), que cruzam `games_summary` com o que já foi carregado.
Logo a sequência é a do `dag_nhl_master` do airflow3:

```bash
uv run python -m pipelines.esportes.nhl.nhl_games_summary     # 1) base dos IDs
dbt build --selector nhl          # 2) no my_datawarehouse: (re)constrói as views
uv run python -m pipelines.esportes.nhl.run_all               # 3) os seis dinâmicos
dbt build --selector nhl          # 4) staging/intermediate/marts com os dados novos
```

`seasons` e `teams` mudam uma vez por ano (rodar em outubro). `run_all` aceita um
source como argumento para rodar um só; todo script aceita `--steps load` para
recarregar o landing sem bater na API (era `--load-only`).

## Configuração

`nhl_config.yml`: no topo, `load: jsonb`, `control_table` e `param_schema`; um
source por endpoint com seu bloco `options` (documentado no cabeçalho do YAML):
`param_view`/`param_columns`/`param_filter` (de onde vêm os IDs), `overwrite`,
`array_key`, `file_pattern`, `season_subdir` e `workers` (threads; default 1, seja
gentil com a API).

Credenciais: só o Postgres do `.env` da raiz (perfil `DB__<ENV>__*`). As tabelas
`raw_nhl.nhl_raw_*` e as views `staging.vw_stg_request_*` ficam no banco do
ambiente (`analytics_dev` em local); o dbt `my_datawarehouse` precisa rodar contra
ele antes dos pipelines dinâmicos. A API não exige token.

## Idempotência

`core.jsonb.JsonbLoader` registra cada arquivo carregado em
`raw_nhl.nhl_ingestion_control` (nome preservado do repo original — o banco já tem o
histórico). Pipelines com `overwrite: false` só inserem arquivos que não constam lá;
os com `overwrite: true` truncam a tabela e limpam o controle antes de recarregar.

## Armadilhas

- **As views precisam existir.** O `my_datawarehouse` está com `staging.nhl`
  `+enabled: false` no `dbt_project.yml`; sem habilitar e construir o seletor `nhl`,
  os dinâmicos falham no `fetch_params`.
- `all_games_summary.json` tem ~33 MB e é serializado em memória antes do COPY.
- A API devolve 404 para jogos ainda não realizados: o `HttpClient` loga e pula.
  A view `vw_stg_request_games_id` já filtra por `has_happened_by_status`.
- `player_game_log` grava em `raw_game_log/<season_id>/` e carrega **só a pasta de
  maior nome** (temporada mais recente). Para recarregar uma temporada antiga, use
  o `JsonbLoader` direto com os arquivos da pasta.
- Volumes: `games_details` e `summary_details` têm ~70 mil arquivos cada; a primeira
  carga completa leva horas. Depois disso é só o delta.
