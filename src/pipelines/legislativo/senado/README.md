# Pipeline: Senado Federal

Extrai dados públicos da [API de Dados Abertos do Senado Federal](https://legis.senado.leg.br/dadosabertos/).
Configuração em `senado_config.yml`.

## O que coleta

| Script | Source (YAML) | Fonte | Tabela destino |
|---|---|---|---|
| `senado_legislatura.py` | `legislatura` | Legislaturas e senadores por legislatura | `raw_senado.legislaturas` |
| `senado_senadores.py` | `senadores` | Perfil dos senadores em exercício | `raw_senado.senadores` |
| `senado_votacoes.py` | `votacoes` | Votações em plenário | `raw_senado.votacoes` |
| `senado_votos_senadores.py` | `votos_senadores` | Como cada senador votou | `raw_senado.votos_senadores` |
| `senado_votos_orientacao.py` | `votos_orientacao` | Orientação de bancada por votação | `raw_senado.votos_orientacao` |
| `senado_status.py` | `status` | Tramitação das proposições do e-Cidadania | `raw_senado.status` |
| `senado_processos.py` | `processo` | Detalhe dos processos das votações | `raw_senado.processo` |

## Dependências entre pipelines

```
senado_votacoes ──> id_votacoes.csv (codigosessaovotacao) ──> senado_votos_senadores
                                                          └─> senado_votos_orientacao
                └─> id_processo.csv (idprocesso) ───────────> senado_processos

e-Cidadania (paginas) ──> ecidadania_paginas_consolidado.csv ──> senado_status
```

⚠️ O `senado_status` depende de **outro pipeline**: ele lê
`ecidadania_paginas_consolidado.csv`, que é o *bronze* gerado por
`ecidadania_paginas`. Rode o e-Cidadania antes, e garanta que o arquivo esteja no
`base_parameters` do Senado (os diretórios de bronze do e-Cidadania e de
parâmetros do Senado são configurados separadamente no YAML).

## Como executar

```bash
uv run python -m pipelines.legislativo.senado.senado_legislatura
uv run python -m pipelines.legislativo.senado.senado_senadores
uv run python -m pipelines.legislativo.senado.senado_votacoes        # primeiro: gera os IDs
uv run python -m pipelines.legislativo.senado.senado_votos_senadores
uv run python -m pipelines.legislativo.senado.senado_votos_orientacao
uv run python -m pipelines.legislativo.senado.senado_processos
uv run python -m pipelines.legislativo.senado.senado_status          # depois do e-Cidadania
```

## Notas

- `senado_votacoes` limpa quebras de linha embutidas em colunas de texto antes de
  gravar o CSV, senão o `read_csv` seguinte quebra com `ParserError`.
- Carga é full refresh (`replace`) na tabela `raw_senado.*`.
