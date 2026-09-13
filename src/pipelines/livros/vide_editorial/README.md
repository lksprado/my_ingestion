# Pipeline: Vide Editorial

Coletor de livros e promoções do site da **Vide Editorial**. Extrai listas de
produtos (home e categorias), pagina automaticamente, salva JSON no lake e carrega
no Postgres. Configuração em `vide_editorial_config.yml`.

## O que coleta

| Script | Source | Fluxo | Destino |
|---|---|---|---|
| `vide_editorial_livros_em_destaque.py` | `livros_em_destaque` | HTML da home → JSON por dia no landing → bronze CSV → tabela | `raw_vide_editorial.livros_em_destaque` |
| `vide_editorial_categorias.py` | `categorias` | Páginas de categoria (`options.hrefs`) → JSON por página | só landing (`load: none`), **sem consumidor hoje** |

`_parsers.py` concentra os seletores BeautifulSoup e a descoberta de paginação.

## Como executar

```bash
uv run python -m pipelines.livros.vide_editorial.vide_editorial_livros_em_destaque
uv run python -m pipelines.livros.vide_editorial.vide_editorial_categorias      # extract-only
```

Saída dos arquivos: `raw/vide/vide_livros_em_destaque_{data}.json` para a home e
`raw/vide/categorias/{categoria}_page_{n}_{data}.json` para as categorias. O bronze
da home concatena **todos** os JSONs do landing (`options.file_pattern`) com a
coluna `source_filename`; a carga é full refresh.

## Configuração de categorias

```yaml
sources:
  categorias:
    options:
      delay: 1.2
      hrefs:
        - name: filosofia
          link: https://videeditorial.com.br/filosofia
```

A paginação é descoberta sozinha (`get_last_page_number`), então basta o link da
primeira página. Entre requisições há um `sleep` de `options.delay` para não
martelar o site.

## Campos extraídos

`name`, `url`, `author_name`, `author_id`, `price_old`, `price_new`, `discount`,
`is_new`, `source`, `created_at` e `category` (só nas páginas de categoria).

## Notas

- Credenciais do banco vêm do `.env` da raiz (perfil `DB__<ENV>__*`); o schema é a
  chave `db_schema` do YAML.
- Se o site mudar o HTML, os seletores a ajustar estão em
  `_parsers.parse_products_page` (`div.item-product`, `.name a.product-name`, etc.).
