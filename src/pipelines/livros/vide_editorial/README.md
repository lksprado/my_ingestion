# Pipeline: Vide Editorial

Coletor de livros e promoções do site da **Vide Editorial**. Extrai listas de
produtos (home e categorias), pagina automaticamente, salva JSON no lake e carrega
no Postgres.

## Fluxo

1. `HttpClient` (de `core`) faz a requisição com retry/backoff.
2. `parsers.py` transforma o HTML numa lista de produtos.
3. `run.py` salva os JSONs no lake e carrega em `raw_vide_editorial.*` via `PostgresClient`.

## Arquivos

| Arquivo | Papel |
|---|---|
| `run.py` | Entrypoint: extração da home, das categorias e carga |
| `parsers.py` | Seletores BeautifulSoup e paginação |
| `config.yml` | Categorias a coletar (`hrefs: [{name, link}]`) |

## Como executar

```bash
# home (livros em destaque) + carga
uv run python -m pipelines.livros.vide_editorial.run
```

Para as demais operações, chame as funções direto:

```bash
# todas as categorias do config.yml
uv run python -c "
from pathlib import Path
from pipelines.livros.vide_editorial.run import extract_categories_content
from settings import settings
extract_categories_content(settings.lake_root / 'raw/vide/categorias')"

# uma categoria específica
uv run python -c "
from pipelines.livros.vide_editorial.run import extract_categories_content
from settings import settings
extract_categories_content(settings.lake_root / 'raw/vide/categorias', only='filosofia')"

# carga de um diretório de JSONs numa tabela
uv run python -c "
from pipelines.livros.vide_editorial.run import load
from settings import settings
load(settings.lake_root / 'raw/vide/categorias', 'categorias')"
```

Saída dos arquivos: `vide_livros_em_destaque_{data}.json` para a home e
`{categoria}_page_{n}_{data}.json` para as categorias.

## Configuração de categorias

```yaml
hrefs:
  - name: filosofia
    link: https://videeditorial.com.br/filosofia
```

A paginação é descoberta sozinha (`get_last_page_number`), então basta o link da
primeira página. Entre requisições há um `sleep` de ~1s para não martelar o site.

## Campos extraídos

`name`, `url`, `author_name`, `author_id`, `price_old`, `price_new`, `discount`,
`is_new`, `source`, `created_at` e `category` (só nas páginas de categoria).

## Notas

- A carga usa `PostgresClient.load_files_to_table`, que concatena todos os JSONs do
  diretório numa tabela só e acrescenta `source_filename`, `arquivo_origem` e
  `data_carga`. É full refresh (`replace`).
- Credenciais do banco vêm do `.env` da raiz (perfil `DB__<ENV>__*`); o schema
  `raw_vide_editorial` é a constante `SCHEMA` de `run.py`, tabela padrão
  `livros_em_destaque`.
- `parsers.get_routes(input_html, output_dir)` é um utilitário manual: recebe um
  HTML salvo pelo DevTools e extrai todos os links do site, útil para descobrir
  novas categorias para o `config.yml`.
- Se o site mudar o HTML, os seletores a ajustar estão em
  `parsers.parse_products_page` (`div.item-product`, `.name a.product-name`, etc.).
