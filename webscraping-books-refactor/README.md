# Webscraping Books Refactor

Um coletor focado em livros e promoções do site da **Vide Editorial**. O projeto extrai listas de produtos (home e categorias), pagina automaticamente, salva em JSON e pode carregar os dados em PostgreSQL.

## Visão geral rápida

- **Coleta resiliente** com `requests` + retry/backoff para erros 4xx/5xx
- **Parsing estruturado** com BeautifulSoup
- **Saídas claras**: arquivos JSON por página e data
- **Carga opcional** para banco com SQLAlchemy + pandas

## Fluxo de dados (em 3 passos)

1. **Extractor** faz a requisição HTTP com retry (`src/extractor.py`).
2. **Parsers** transformam o HTML em uma lista de produtos (`src/parsers.py`).
3. **Persistência** salva JSON localmente ou faz carga no Postgres (`src/loader.py`).

## Estrutura do projeto

```
.
├── src/
│   ├── extractor.py
│   ├── parsers.py
│   ├── loader.py
│   ├── main.py
│   └── config.yml
├── data/
├── docs/
├── tests/
└── README.md
```

## Como rodar

### 1) Instalação

```
uv sync --no-install-project
```

### 2) Extração de destaques (home)

```bash
uv run python -c "from src.main import extraction_featured_books; extraction_featured_books('data/out')"
```

Saída esperada (exemplo):

```
vide_livros_em_destaque_2026-01-26.json
```

### 3) Extração de categorias (config.yml)

O arquivo `src/config.yml` define as categorias e URLs que serão coletadas:

```yml
hrefs:
  - name: filosofia
    link: https://videeditorial.com.br/filosofia
```

Para extrair **todas** as categorias:

```bash
uv run python -c "from src.main import extract_link_content; extract_link_content('data/out', 'src/config.yml')"
```

Para extrair **uma categoria específica**:

```bash
uv run python -c "from src.main import extract_one_category_content; extract_one_category_content('filosofia', 'data/out', 'src/config.yml')"
```

Saída esperada (exemplo):

```
filosofia_page_1_2026-01-26.json
filosofia_page_2_2026-01-26.json
...
```

## Carga no Postgres

A carga usa as variáveis de ambiente abaixo:

- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PW`

Exemplo de carga:

```bash
uv run python -c "from src.loader import load_data; load_data('data/out', 'json', 'raw', 'vide_raw_home_featured')"
```

## Campos extraídos

- `name`, `url`
- `author_name`, `author_id`
- `price_old`, `price_new`
- `discount`, `is_new`
- `source`, `created_at`
- `category` (quando aplicável)

## Observabilidade

Logs de execução seguem um formato padrão e podem ser configurados para arquivo em `src/utils/log.py`.

## Qualidade

```
uv run pytest
uv run ruff check .
uv run ruff fix .
```

## Dicas de evolução

- Adicione novas categorias no `src/config.yml`.
- Ajuste os seletores em `src/parsers.py` caso o HTML do site mude.
- Se quiser um CLI, o ponto de partida é `src/main.py`.
