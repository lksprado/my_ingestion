# Pipeline: Senado Federal

Extrai dados públicos da [API de Dados Abertos do Senado Federal](https://legis.senado.leg.br/dadosabertos/).
ETL em `senado_etl.py` (todas as entidades); configuração em `senado_config.yml`.

## O que coleta

| Entidade | Fonte | Tabela destino |
|---|---|---|
| `legislaturas` | Senadores da legislatura atual | `raw_senado.legislaturas` |
| `senadores` | Perfil dos senadores em exercício | `raw_senado.senadores` |
| `votacoes` | Votações em plenário (2001..) | `raw_senado.votacoes` |
| `votos_senadores` | Como cada senador votou | `raw_senado.votos_senadores` |
| `votos_orientacao` | Orientação de bancada por votação | `raw_senado.votos_orientacao` |
| `processo` | Detalhe dos processos das votações | `raw_senado.processo` |
| `status` | Tramitação das proposições do e-Cidadania | `raw_senado.status` |

## Dependências entre pipelines

```
votacoes ──> landing de votacoes ─────────────────────────────> votos_senadores (sem extract próprio)
         └─> id_processo.csv (idprocesso) ───────────────────> processo

e-Cidadania (paginas) ──> ecidadania_paginas_consolidado.csv ──> status
```

⚠️ `status` depende de **outra fonte**: lê `ecidadania_paginas_consolidado.csv`,
o *bronze* de `ecidadania_etl paginas`, a partir do `base_parameters` do Senado. Nenhum
YAML declara esse vínculo: rode o e-Cidadania antes e copie o arquivo.

## Como executar

`senado_etl [entidade ...] [--steps extract,transform,load]`; sem entidades roda
todas na ordem acima (a falha de uma não aborta as demais).

```bash
uv run python -m pipelines.legislativo.senado.senado_etl                            # todas
uv run python -m pipelines.legislativo.senado.senado_etl votacoes votos_senadores   # só essas
uv run python -m pipelines.legislativo.senado.senado_etl status                     # depois do e-Cidadania
```

## Notas

- `votacoes` e `votos_orientacao` varrem 2001..2026 a cada extract
  (`YEARS` e `extract_by_year` no script). Antes da padronização rodavam etapas parciais (só transform /
  só load); agora `--steps` faz esse papel.
- `processo` é incremental por ID (`core.extract_by_ids`, default `has_data=bool`)
  e reconstrói o bronze em streaming.
- Quebras de linha em colunas de texto são removidas por `core.write_bronze`.
- Carga é full refresh (`replace`) na tabela `raw_senado.*`.
