# Pipeline: Ranking Políticos

Extrai o ranking de desempenho parlamentar da [API do Politicos.org.br](https://apirest2.politicos.org.br/api/),
que pontua parlamentares por critérios como presença, economia de cota, processos e
privilégios. Configuração em `ranking_politicos_config.yml`; extract/transform em
`_common.py` (os dois scripts só escolhem o source).

## O que coleta

| Script | Source (YAML) | Fonte | Tabela destino |
|---|---|---|---|
| `ranking_deputados.py` | `deputados` | Ranking e score dos deputados | `raw_ranking_politicos.deputados` |
| `ranking_senadores.py` | `senadores` | Ranking e score dos senadores | `raw_ranking_politicos.senadores` |

## Como executar

```bash
uv run python -m pipelines.legislativo.ranking_politicos.ranking_deputados
uv run python -m pipelines.legislativo.ranking_politicos.ranking_senadores
```

Não há dependência entre os dois nem com outros pipelines.

## Notas

- A extração é por ano: `landing_file` usa `{year}` (`ranking_deputados_<ano>.json`)
  e a transformação concatena **todos** os JSONs do landing, preservando o histórico.
