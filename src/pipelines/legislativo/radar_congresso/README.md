# Pipeline: Radar Congresso

Extrai dados do [Radar Congresso em Foco](https://radar.congressoemfoco.com.br/),
que calcula o índice de governismo dos parlamentares a partir dos votos em plenário.
Configuração em `radar_congresso_config.yml`; o transform do governismo (wide
trimestral → long) fica em `_common.py`.

## O que coleta

| Script | Source (YAML) | Fonte | Tabela destino |
|---|---|---|---|
| `radar_governismo_deputados.py` | `governismo_deputados` | Índice de governismo dos deputados | `raw_radar_congresso.governismo_deputados` |
| `radar_governismo_senadores.py` | `governismo_senadores` | Índice de governismo dos senadores | `raw_radar_congresso.governismo_senadores` |
| `radar_parlamentares.py` | `parlamentares` | Cadastro de parlamentares | `raw_radar_congresso.parlamentares` |

Os três usam o extract padrão da `core` (uma requisição a `base_url`).

## Como executar

```bash
uv run python -m pipelines.legislativo.radar_congresso.radar_governismo_deputados
uv run python -m pipelines.legislativo.radar_congresso.radar_governismo_senadores
uv run python -m pipelines.legislativo.radar_congresso.radar_parlamentares
```

## Notas

- ⚠️ A camada staging deste pipeline está **desabilitada no dbt**: o
  `dbt_project.yml` do `demodadosdw` traz `staging.radar_congresso.+enabled: false`.
  A ingestão popula as tabelas `raw_radar_congresso.*` normalmente, mas nada é modelado a jusante
  enquanto essa flag não mudar.
