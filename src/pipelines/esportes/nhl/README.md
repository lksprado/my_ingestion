# Pipeline: NHL

Extrai estatísticas de hóquei das APIs públicas da NHL (`api-web.nhle.com` e
`api.nhle.com/stats`) e carrega os JSONs **sem transformação** em tabelas JSONB
(`payload`, `source_filename`, `loaded_at_utc`) no schema `raw_nhl` (`load: jsonb` no YAML). A
normalização acontece no dbt [`my_analytics`](https://github.com/lksprado/my_analytics),
que consome `raw_nhl` **a jusante**: nenhum passo deste pipeline depende dele.

Migrado do repo `nhl-extraction` (submódulo `include/nhl_extraction` do airflow3).

## O que coleta

ETL em `nhl_etl.py` (todas as entidades); configuração em `nhl_config.yml`.

| Entidade | Tabela | Carga |
|---|---|---|
| `seasons` | `raw_nhl.nhl_raw_all_seasons_id` | full (anual) |
| `teams` | `raw_nhl.nhl_raw_all_teams_id` | full (anual) |
| `games_summary` | `raw_nhl.nhl_raw_all_games_summary` | full (diário) |
| `games_summary_details` | `raw_nhl.nhl_raw_all_games_summary_details` | incremental |
| `games_details` | `raw_nhl.nhl_raw_all_games_details` | incremental |
| `play_by_play` | `raw_nhl.nhl_raw_all_play_by_play` | incremental |
| `club_stats` | `raw_nhl.nhl_raw_all_club_stats` | full |
| `player_game_log` | `raw_nhl.nhl_raw_all_player_game_log` | full (temporada mais recente) |
| `players` | `raw_nhl.nhl_raw_all_players` | full |

Os nomes de tabela e de pasta (`raw/nhl/single`, `raw/nhl/raw_all_games_details`…)
**fogem do padrão** `raw_<fonte>.<entidade>` do monorepo de propósito: o dbt os
referencia em `_sources.yml` e o lake já tem ~170 mil JSONs nesses diretórios. Só
o schema segue o padrão (`raw_nhl`, chave `db_schema` do YAML).

## Como funciona

Em `ETLS`, as entidades **estáticas** usam o extract padrão da `core` (uma requisição
a `base_url`); as **dinâmicas** usam `extract_dynamic`, que recebe por `functools.partial`
a função `params_*` que descobre quais IDs requisitar e depois pede
`base_url.format(**linha)`. Não há transform. O load é o modo `jsonb` da `core`
(`JsonbLoader`, controle em `raw_nhl.nhl_ingestion_control`); só `player_game_log`
usa um `load_fn` próprio para carregar a temporada mais recente.

## De onde vêm os IDs (sem dbt)

As quatro consultas de parâmetro leem o **mesmo banco da carga** (`PostgresClient()`,
não `models_target`), só o schema `raw_nhl`:

| Função | Entidades | O que devolve |
|---|---|---|
| `params_jogos` | `games_details`, `games_summary_details`, `play_by_play` | `game_id` dos jogos já realizados (`gameStateId = 7`) na temporada atual que ainda não constam de `cfg.db_table` na tabela de controle |
| `params_times` | `club_stats` | `team_id` (triCode), `season_id`, `game_type_id` dos times com jogo realizado na temporada atual, tipos 2 e 3 |
| `params_jogadores` | `players` | `player_id` de goleiros e jogadores de linha em `nhl_raw_all_club_stats` na temporada/tipo atuais |
| `params_jogadores_temporada` | `player_game_log` | os mesmos `player_id` com `season_id`/`game_type_id` |

Elas substituem as views `staging_nhl.vw_stg_request_*` do `my_analytics` e devolvem
o mesmo conjunto (conferido linha a linha contra as views). A diferença é o critério
de "já tenho": antes era "existe no staging do dbt", agora é "existe na tabela de
controle da ingestão" — o metadado que o próprio `JsonbLoader` grava.

`params_jogadores*` derivam a temporada da **agenda** (`games_summary`), não de
`nhl_raw_all_seasons_id`: o endpoint de seasons já lista a temporada seguinte, que
ainda não tem `club_stats` e zeraria a lista de jogadores. É o mesmo critério que as
views usavam.

## Ordem de execução

`games_summary` é a base de tudo (é dela que saem os jogos realizados); `teams`
traduz id → triCode para `club_stats`; `club_stats` alimenta `players` e
`player_game_log`. Sem dbt no meio:

```bash
uv run python -m pipelines.esportes.nhl.nhl_etl games_summary     # 1) base dos IDs
uv run python -m pipelines.esportes.nhl.nhl_etl \
    games_summary_details games_details play_by_play \
    club_stats player_game_log players                            # 2) os seis dinâmicos
```

`seasons` e `teams` mudam uma vez por ano (rodar em outubro:
`nhl_etl seasons teams`). Rodar `nhl_etl` sem entidades executa as nove na ordem de
`ETLS`, que já é essa. Uma entidade que falha não aborta as demais. `--steps load`
recarrega o landing sem bater na API.

## Configuração

`nhl_config.yml`: no topo, `load: jsonb` e `control_table`; um source por endpoint
com seu bloco `options` (documentado no cabeçalho do YAML): `overwrite`, `array_key`,
`file_pattern`, `season_subdir`, `skip_existing` e `workers` (threads; default 1, seja
gentil com a API). **Quais IDs requisitar não está no YAML** — é a `params_*` amarrada
em `ETLS`.

Credenciais: só o Postgres do `.env` da raiz (perfil `DB__<ENV>__*`). Em dev tudo
acontece no `ingestion_sandbox`; em prod, no banco do Airflow. Para testar um
incremental no sandbox, antes rode `scripts/raw_copy.sh seed raw_nhl` — senão o
controle de ingestão está vazio e os `params_*` pedem a temporada inteira.

## Backfill de uma temporada passada

`params_jogos` olha para a temporada atual (maior id em `nhl_raw_all_seasons_id`).
Quando a API publica a temporada seguinte — o que acontece meses antes do primeiro
jogo —, o que ficou faltando da anterior sai do escopo e nunca mais é pedido. Para
fechar esse buraco, passe `options.season_id`:

```python
from core import GenericETL, PipelineConfig, setup_logger
from pipelines.esportes.nhl.nhl_etl import CONFIG_FILE, extract_dynamic, params_jogos

setup_logger()
for entidade in ("games_details", "games_summary_details", "play_by_play"):
    cfg = PipelineConfig.from_yaml(CONFIG_FILE, entidade)
    cfg.options["season_id"] = 20252026
    extract_dynamic(cfg, params=params_jogos)
    GenericETL(cfg).load()
```

Não dá para usar `build_etl` aqui: ele relê o YAML e perderia o `season_id`.

O load não precisa de `season_id`: o `JsonbLoader` pega do landing o que ainda não
está na tabela de controle. `skip_existing` continua valendo, então o que já foi
baixado não é rebaixado.

## Idempotência

`core.jsonb.JsonbLoader` registra cada arquivo carregado em
`raw_nhl.nhl_ingestion_control` (nome preservado do repo original — o banco já tem o
histórico). Pipelines com `overwrite: false` só inserem arquivos que não constam lá;
os com `overwrite: true` truncam a tabela e limpam o controle antes de recarregar.
Como os `params_*` leem esse mesmo controle, extract e load enxergam o mesmo delta.

## Armadilhas

- **`table_schema` legado no controle.** O repo `nhl-extraction` gravava as ~168 mil
  linhas de controle com `table_schema = 'nhl'`; aqui o schema é `raw_nhl`. Enquanto
  isso não for corrigido, o controle parece vazio: a carga duplica tudo e os `params_*`
  pedem a temporada inteira de novo. Rode uma vez por banco:
  `scripts/nhl_controle_migra.sh` (idempotente).
- `all_games_summary.json` tem ~33 MB e é serializado em memória antes do COPY.
- A API devolve 404 para jogos ainda não realizados: o `HttpClient` loga e pula.
  O `params_jogos` já filtra por `gameStateId = 7`.
- `player_game_log` grava em `raw_game_log/<season_id>/` e carrega **só a pasta de
  maior nome** (temporada mais recente). Para recarregar uma temporada antiga, use
  o `JsonbLoader` direto com os arquivos da pasta.
- Na entressafra, `players` e `player_game_log` devolvem zero parâmetros: a agenda já
  tem a temporada seguinte, mas `club_stats` ainda é da anterior. Volta ao normal no
  primeiro jogo realizado. Era assim nas views do dbt também.
- Volumes: `games_details` e `summary_details` têm ~70 mil arquivos cada; a primeira
  carga completa leva horas. Depois disso é só o delta — e `skip_existing` evita
  rebaixar o que já está no landing.
