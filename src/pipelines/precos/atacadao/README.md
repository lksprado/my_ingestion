# Pipeline: Preços Atacadão (inflação pessoal)

Coleta periódica de preços de produtos do mercado onde eu compro, para calcular uma
variação de preços **personalizada** — uma "inflação" da minha cesta real, em vez de
um índice geral.

## Como funciona

O site do Atacadão expõe uma API GraphQL de busca. O `scraper.py` monta a query por
keyword e loja, e o `run.py` percorre o produto cartesiano lojas × keywords,
gravando um CSV (`;`) por combinação:

```
${LAKE_ROOT}/raw/inflation/atacadao/{store_id}_{keyword}_{YYYY-MM-DD}.csv
```

Campos extraídos: `store_id`, `sku`, `category`, `sub_category`, `product_name`,
`brand_name`, `high_price`, `low_price`, mais `keyword` e `extracted_at`.

## Configuração

| Arquivo | Conteúdo |
|---|---|
| `store_config.yml` | Lojas (`region_id`, `sales_channel`, `seller`, `locale`, `search_url`, `operation`). Lojas com `enabled: false` são puladas |
| `products_config.yml` | Lista de keywords sob `keywords:` — aceita string simples ou `{name: ...}` |

Adicionar um produto à cesta é uma linha em `products_config.yml`.

## Como executar

```bash
uv run python -m pipelines.precos.atacadao.run        # coleta do dia
uv run python -m pipelines.precos.atacadao.historic   # consolida para o seed do dbt
```

O `historic.py` concatena os CSVs mensais de
`${LAKE_ROOT}/bronze/inflation/months/` e grava `minha_inflacao.csv` em
`${SEEDS_ROOT}` — ou seja, entrega direto como seed do data warehouse
[`the_dw`](https://github.com/lksprado/the_dw), onde a modelagem acontece.

## Notas

- Não há carga em banco aqui: a saída é CSV, e o dbt do `the_dw` assume daí.
- O `HttpClient` é configurado com retry mais curto (3 tentativas, backoff 0.5) por
  ser scraping de site. Keywords sem resultado são simplesmente puladas.
- A URL da API embute JSON dentro de query params; se a busca voltar vazia para
  tudo, o mais provável é que o `operation` ou o formato de `selectedFacets` tenha
  mudado no site.
