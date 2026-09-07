# Pipeline: Radar Congresso

Extrai dados do [Radar Congresso em Foco](https://radar.congressoemfoco.com.br/),
que calcula o índice de governismo dos parlamentares a partir dos votos em plenário.
Configuração em `radar_congresso_config.yml`.

## O que coleta

| Script | Source (YAML) | Fonte | Tabela destino |
|---|---|---|---|
| `radar_governismo.py` | `governismo_deputados` | Índice de governismo dos deputados | `raw_radar_governismo_deputados` |
| `radar_governismo.py` | `governismo_senadores` | Índice de governismo dos senadores | `raw_radar_governismo_senadores` |
| `radar_parlamentares.py` | `parlamentares` | Cadastro de parlamentares | `raw_radar_parlamentares` |

`radar_governismo.py` atende **duas** sources (deputados e senadores) — a casa é
escolhida no `__main__` do script.

## Como executar

```bash
uv run python -m pipelines.legislativo.radar_congresso.radar_governismo
uv run python -m pipelines.legislativo.radar_congresso.radar_parlamentares
```

## Notas

- ⚠️ A camada staging deste pipeline está **desabilitada no dbt**: o
  `dbt_project.yml` do `demodadosdw` traz `staging.radar_congresso.+enabled: false`.
  A ingestão popula as tabelas `raw.*` normalmente, mas nada é modelado a jusante
  enquanto essa flag não mudar.
