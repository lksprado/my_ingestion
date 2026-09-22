# Pipeline: e-Cidadania

Extrai dados do portal [e-Cidadania do Senado Federal](https://www12.senado.leg.br/ecidadania/),
onde cidadãos apoiam ideias e proposições legislativas. É **scraping de HTML** (não
há API), então os seletores são sensíveis a mudanças no site.
ETL em `ecidadania_etl.py` (parsers inclusos); configuração em `ecidadania_config.yml`.

## O que coleta

| Entidade | Fonte | Tabela destino |
|---|---|---|
| `bignumbers` | Totais gerais de apoios e proposições | `raw_ecidadania.raw_ecidadania_bignumbers` |
| `mais_votados` | Proposições com mais apoios | `raw_ecidadania.raw_ecidadania_mais_votados` |
| `paginas` | Lista paginada de todas as proposições | `raw_ecidadania.raw_ecidadania_paginas` |

## Dependência a jusante

`paginas` gera o bronze `ecidadania_paginas_consolidado.csv`, que alimenta o
**`senado_etl status`** (ver `../senado/README.md`). Rode este pipeline antes.

## Como executar

```bash
uv run python -m pipelines.legislativo.ecidadania.ecidadania_etl            # as três
uv run python -m pipelines.legislativo.ecidadania.ecidadania_etl paginas    # só uma
```

## Notas

- **Dívida registrada:** o extract parseia o HTML e grava o **CSV** no landing; o
  HTML bruto não é guardado, então reprocessar após mudança de seletor exige nova
  extração.
- `paginas` lê as primeiras `options.pages` páginas (25) de
  `pesquisamateria?p=N`: 100 matérias por página, ordenadas por votos, de 77 páginas
  hoje. Se uma página vier vazia, para ali. **Armadilha:** `principalmateria?p=N`
  ignora o `p` e devolve sempre as mesmas 3 matérias em destaque; não volte a ela.
- `paginas` e `mais_votados` usam o mesmo parser (`parse_materias`). Os parsers
  dependem de `#container-consulta-publica`: quando o site muda, logam warning e
  devolvem DataFrame vazio — confira o log quando a carga vier vazia.
- Testes dos parsers: `tests/legislativo/test_ecidadania_parsers.py`.
