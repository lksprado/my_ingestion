# Pipeline: Vide Editorial

Coletor de livros e promoções do site da **Vide Editorial**. Extrai listas de
produtos (home e categorias), pagina automaticamente, salva JSON no lake e carrega
no Postgres. ETL em `vide_editorial_etl.py` (parsers inclusos); configuração em
`vide_editorial_config.yml`.

## O que coleta

| Entidade | Fluxo | Destino |
|---|---|---|
| `livros_em_destaque` | HTML da home → JSON por dia no landing → bronze CSV → tabela | `raw_vide_editora.vide_raw_home_featured` |
| `categorias` | Páginas de categoria (`options.hrefs`) → JSON por página | só landing (`load: none`), **sem consumidor hoje** |

`parse_products_page` e `get_last_page_number` concentram os seletores BeautifulSoup e a descoberta de paginação.

## Como executar

```bash
uv run python -m pipelines.livros.vide_editorial.vide_editorial_etl livros_em_destaque
uv run python -m pipelines.livros.vide_editorial.vide_editorial_etl categorias      # extract-only
```

Saída dos arquivos: `raw/vide/destaques/vide_livros_em_destaque_{data}.json` para a
home e `raw/vide/paginas/{categoria}_page_{n}_{data}.json` para as categorias. O bronze
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
  `parse_products_page` (`div.item-product`, `.name a.product-name`, etc.).
