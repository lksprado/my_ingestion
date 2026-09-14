# Pipeline: Ranking Políticos

Extrai o ranking de desempenho parlamentar da [API do Politicos.org.br](https://apirest2.politicos.org.br/api/),
que pontua parlamentares por critérios como presença, economia de cota, processos e
privilégios. ETL em `ranking_politicos_etl.py` (as duas entidades usam o mesmo
extract/transform); configuração em `ranking_politicos_config.yml`.

## O que coleta

| Entidade | Fonte | Tabela destino |
|---|---|---|
| `deputados` | Ranking e score dos deputados | `raw_ranking_politicos.raw_ranking_deputados` |
| `senadores` | Ranking e score dos senadores | `raw_ranking_politicos.raw_ranking_senadores` |

## Como executar

```bash
uv run python -m pipelines.legislativo.ranking_politicos.ranking_politicos_etl             # as duas
uv run python -m pipelines.legislativo.ranking_politicos.ranking_politicos_etl senadores   # só uma
```

Não há dependência entre os dois nem com outros pipelines.

## Notas

- A extração é por ano: `landing_file` usa `{year}` (`ranking_deputados_<ano>.json`)
  e a transformação concatena **todos** os JSONs do landing, preservando o histórico.
