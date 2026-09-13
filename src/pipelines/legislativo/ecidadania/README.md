# Pipeline: e-Cidadania

Extrai dados do portal [e-Cidadania do Senado Federal](https://www12.senado.leg.br/ecidadania/),
onde cidadãos apoiam ideias e proposições legislativas. É **scraping de HTML** (não
há API), então os seletores são sensíveis a mudanças no site.
Configuração em `ecidadania_config.yml`; parsers em `_parsers.py`, extract/transform
comuns em `_common.py`.

## O que coleta

| Script | Source (YAML) | Fonte | Tabela destino |
|---|---|---|---|
| `ecidadania_bignumbers.py` | `bignumbers` | Totais gerais de apoios e proposições | `raw_ecidadania.bignumbers` |
| `ecidadania_mais_votados.py` | `mais_votados` | Proposições com mais apoios | `raw_ecidadania.mais_votados` |
| `ecidadania_paginas.py` | `paginas` | Lista paginada de todas as proposições | `raw_ecidadania.paginas` |

## Dependência a jusante

`ecidadania_paginas` gera o bronze `ecidadania_paginas_consolidado.csv`, que
alimenta o **`senado_status`** (ver `../senado/README.md`). Rode este pipeline antes.

## Como executar

```bash
uv run python -m pipelines.legislativo.ecidadania.ecidadania_bignumbers
uv run python -m pipelines.legislativo.ecidadania.ecidadania_mais_votados
uv run python -m pipelines.legislativo.ecidadania.ecidadania_paginas
```

## Notas

- **Dívida registrada:** o extract parseia o HTML e grava o **CSV** no landing; o
  HTML bruto não é guardado, então reprocessar após mudança de seletor exige nova
  extração.
- `ecidadania_paginas` varre `options.pages` páginas (145 hoje); revise no YAML se
  o volume de proposições crescer.
- `paginas` e `mais_votados` usam o mesmo parser (`parse_materias`). Os parsers
  dependem de `#container-consulta-publica`: quando o site muda, logam warning e
  devolvem DataFrame vazio — confira o log quando a carga vier vazia.
- Testes: `tests/legislativo/test_ecidadania_parsers.py`.
