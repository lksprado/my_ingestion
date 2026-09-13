# Pipeline: e-Cidadania

Extrai dados do portal [e-Cidadania do Senado Federal](https://www12.senado.leg.br/ecidadania/),
onde cidadãos apoiam ideias e proposições legislativas. É **scraping de HTML** (não
há API), então os seletores são sensíveis a mudanças no site.
Configuração em `ecidadania_config.yml`.

## O que coleta

| Script | Source (YAML) | Fonte | Tabela destino |
|---|---|---|---|
| `ecidadania_big_numbers.py` | `big_numbers` | Totais gerais de apoios e proposições | `raw_ecidadania.bignumbers` |
| `ecidadania_mais_votados.py` | `mais_votados` | Proposições com mais apoios | `raw_ecidadania.mais_votados` |
| `ecidadania_paginas.py` | `paginas` | Lista paginada de todas as proposições | `raw_ecidadania.paginas` |

## Dependência a jusante

`ecidadania_paginas` gera o bronze `ecidadania_paginas_consolidado.csv`, que
alimenta o **`senado_status`** (ver `../senado/README.md`). Rode este pipeline antes.

## Como executar

```bash
uv run python -m pipelines.legislativo.ecidadania.ecidadania_big_numbers
uv run python -m pipelines.legislativo.ecidadania.ecidadania_mais_votados
uv run python -m pipelines.legislativo.ecidadania.ecidadania_paginas
```

## Notas

- `ecidadania_paginas` varre um intervalo fixo de páginas do portal; se o volume de
  proposições crescer, esse limite precisa ser revisto no código.
- Os parsers dependem do elemento `#container-consulta-publica`. Quando o site muda,
  o pipeline loga um warning e devolve DataFrame vazio em vez de quebrar — vale
  conferir o log quando a carga vier vazia.
