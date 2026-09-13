# Pipeline: Senado Federal

Extrai dados públicos da [API de Dados Abertos do Senado Federal](https://legis.senado.leg.br/dadosabertos/).
Configuração em `senado_config.yml`; helpers do domínio em `../_common.py`.

## O que coleta

| Script | Source (YAML) | Fonte | Tabela destino |
|---|---|---|---|
| `senado_legislaturas.py` | `legislaturas` | Senadores da legislatura atual | `raw_senado.legislaturas` |
| `senado_senadores.py` | `senadores` | Perfil dos senadores em exercício | `raw_senado.senadores` |
| `senado_votacoes.py` | `votacoes` | Votações em plenário (2001..) | `raw_senado.votacoes` |
| `senado_votos_senadores.py` | `votos_senadores` | Como cada senador votou | `raw_senado.votos_senadores` |
| `senado_votos_orientacao.py` | `votos_orientacao` | Orientação de bancada por votação | `raw_senado.votos_orientacao` |
| `senado_status.py` | `status` | Tramitação das proposições do e-Cidadania | `raw_senado.status` |
| `senado_processo.py` | `processo` | Detalhe dos processos das votações | `raw_senado.processo` |

## Dependências entre pipelines

```
senado_votacoes ──> landing de votacoes ─────────────────────> senado_votos_senadores (sem extract próprio)
                └─> id_processo.csv (idprocesso) ───────────> senado_processo

e-Cidadania (paginas) ──> ecidadania_paginas_consolidado.csv ──> senado_status
```

⚠️ `senado_status` depende de **outro pipeline**: lê `ecidadania_paginas_consolidado.csv`,
o *bronze* de `ecidadania_paginas`, a partir do `base_parameters` do Senado. Nenhum
YAML declara esse vínculo: rode o e-Cidadania antes e copie o arquivo.

## Como executar

Todo script aceita `--steps extract,transform,load` (default: todas).

```bash
uv run python -m pipelines.legislativo.senado.senado_legislaturas
uv run python -m pipelines.legislativo.senado.senado_senadores
uv run python -m pipelines.legislativo.senado.senado_votacoes        # primeiro: gera os IDs e o landing
uv run python -m pipelines.legislativo.senado.senado_votos_senadores
uv run python -m pipelines.legislativo.senado.senado_votos_orientacao
uv run python -m pipelines.legislativo.senado.senado_processo
uv run python -m pipelines.legislativo.senado.senado_status          # depois do e-Cidadania
```

## Notas

- `senado_votacoes` e `senado_votos_orientacao` varrem 2001..2026 a cada extract
  (`YEARS` no script). Antes da padronização rodavam etapas parciais (só transform /
  só load); agora `--steps` faz esse papel.
- `senado_processo` é incremental por ID (`_common.extract_by_ids`, `has_data=bool`)
  e reconstrói o bronze em streaming.
- Quebras de linha em colunas de texto são removidas por `core.write_bronze`.
- Carga é full refresh (`replace`) na tabela `raw_senado.*`.
