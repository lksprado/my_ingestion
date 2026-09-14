# Pipeline: Radar Congresso

Extrai dados do [Radar Congresso em Foco](https://radar.congressoemfoco.com.br/),
que calcula o índice de governismo dos parlamentares a partir dos votos em plenário.
ETL em `radar_congresso_etl.py`; configuração em `radar_congresso_config.yml`. O
governismo de deputados e senadores compartilha o mesmo transform (wide trimestral → long).

## O que coleta

| Entidade | Fonte | Tabela destino |
|---|---|---|
| `governismo_deputados` | Índice de governismo dos deputados | `raw_radar_congresso.raw_radar_governismo_deputados` |
| `governismo_senadores` | Índice de governismo dos senadores | `raw_radar_congresso.raw_radar_governismo_senadores` |
| `parlamentares` | Cadastro de parlamentares | `raw_radar_congresso.raw_radar_parlamentares` |

Os três usam o extract padrão da `core` (uma requisição a `base_url`).

## Como executar

```bash
uv run python -m pipelines.legislativo.radar_congresso.radar_congresso_etl                 # as três
uv run python -m pipelines.legislativo.radar_congresso.radar_congresso_etl parlamentares   # só uma
```

## Notas

- ⚠️ A camada staging deste pipeline está **desabilitada no dbt**: o
  `dbt_project.yml` do `demodadosdw` traz `staging.radar_congresso.+enabled: false`.
  A ingestão popula as tabelas `raw_radar_congresso.*` normalmente, mas nada é modelado a jusante
  enquanto essa flag não mudar.
