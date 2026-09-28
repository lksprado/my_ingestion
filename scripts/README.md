# `scripts/` — manutenção

Scripts de uso pontual, fora do fluxo dos pipelines. Cada um explica no próprio
cabeçalho o que faz, as opções e os cuidados; este arquivo só diz qual usar.
Rode da raiz do repo.

| Script | Para quê | Quando |
|---|---|---|
| `raw_copy.sh {seed\|promote\|pull} raw_<fonte>` | Copia um schema raw entre bancos, tabela a tabela (`TRUNCATE` + `COPY`, nunca `DROP`). `seed`: `analytics_dev` → sandbox; `promote`: sandbox → `analytics_dev`; `pull`: prod → `analytics_dev`. | `seed` antes de testar um incremental em dev; `promote` quando a raw do sandbox está validada; `pull` para a raw de dev não envelhecer. `--dry-run` mostra o que faria. |
| `validar_configs.py [arquivo ...]` | Valida a estrutura dos `<fonte>_config.yml` e confere as sources contra o `ETLS`. | Roda sozinho no pre-commit e no CI; à mão ao mexer em YAML. |
| `controle_semear.py <config.yml> <entidade>` | Registra na tabela de controle o landing que a raw já contém. | **Uma vez**, ao ligar `write: append` + `options.control_table` numa tabela com histórico. Sem isso o primeiro delta duplica a tabela. |
| `controle_reconstruir.py <config.yml> <entidade>` | Refaz uma tabela incremental a partir do landing inteiro, numa transação. | Quando o que já entrou está errado (coluna perdida, duplicata). **Sem `--confirmar` só simula.** |
| `camara_votacoes_backfill.py --desde <ano>` | Rebaixa as votações da Câmara de todos os trimestres desde um ano. | Para fechar lacunas no histórico de `votacoes`. Rode onde está o landing que alimenta a raw. |
| `drop_tabelas_orfas.sh {models\|prod}` | Derruba as tabelas `*_tipos_*` que sobraram na raw (hoje são seeds do dbt). | Limpeza. **Sem `--confirmar` só simula**, e pula tabela com dependente ou sem o dado preservado no seed. |

Os que acessam o banco usam os perfis `DB__<ENV>__*` do `.env` da raiz. Em prod, os
scripts Python rodam no container do scheduler do Airflow, sem `uv`; o comando está
no cabeçalho do `controle_reconstruir.py`.
