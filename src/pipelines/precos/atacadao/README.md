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
[`my_analytics`](https://github.com/lksprado/my_analytics), onde a modelagem acontece.

## Notas

- Não há carga em banco aqui: a saída é CSV, e o dbt do `my_analytics` assume daí. Por
  isso é exceção consciente ao `GenericETL` (usa `HttpClient`, `load_yaml`,
  `concat_files_to_df`, `write_csv` e `setup_logger` da `core`).
- ⚠️ Nada neste repo produz `bronze/inflation/months/` (entrada do `historic.py`),
  e o separador difere (`;` na coleta, `,` na leitura): a consolidação mensal é
  manual/externa.
- O `HttpClient` é configurado com retry mais curto (3 tentativas, backoff 0.5) por
  ser scraping de site. Keywords sem resultado são simplesmente puladas.
- Cada keyword busca **duas páginas** de 100 itens (`after=0` e `after=100`), com
  deduplicação por SKU; a segunda página é pulada se a primeira vier vazia. A API
  devolve `null` (não ausência) em campos vazios, daí os `or {}` no parser.
- A URL da API embute JSON dentro de query params; se a busca voltar vazia para
  tudo, o mais provável é que o `operation` ou o formato de `selectedFacets` tenha
  mudado no site.
