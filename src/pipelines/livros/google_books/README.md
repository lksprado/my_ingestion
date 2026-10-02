# Google Books

Enriquece os livros da biblioteca pessoal com dados da
[Google Books API](https://developers.google.com/books/docs/v1/using) (`GET /books/v1/volumes`),
buscando cada livro por título e autor. ETL em `google_books_etl.py`; configuração em
`google_books_config.yml`.

## O que coleta

| Entidade | Fluxo | Destino |
|---|---|---|
| `livros` | Livros da seed → um JSON por livro no landing → bronze CSV → tabela | `raw_google_books.livros` |

Uma linha por livro encontrado: `titulo` e `autor` (como estão na seed), `busca` (qual tentativa
achou, ver abaixo) e `id`, `title`, `subtitle`, `authors`, `publisher`, `published_date`,
`page_count`, `categories`, `language`, `isbn_10`, `isbn_13`, `description`, `thumbnail` e
`info_link` do item aceito.

## Dependências

Os livros vêm da seed do dbt `seeds.seed_biblioteca` (colunas `livros` e `autor`), lida de
`settings.models_target` — `analytics_dev` em dev. Rode o `dbt seed` antes quando a lista mudar.
Tabela e colunas ficam em `options.seed_table`/`seed_column`/`author_column`.

## Como a busca funciona

Três tentativas, da mais restrita para a mais solta, com até 5 itens cada; para na primeira que
tiver um item **cujo autor confere** com a seed:

| `busca` | Consulta |
|---|---|
| `autor` | `intitle:"<título>" inauthor:"<primeiro autor>"` |
| `sobrenome` | `intitle:"<título>" inauthor:<sobrenome do primeiro autor>` |
| `titulo` | `intitle:"<título>"` |

"Confere" é o sobrenome de algum autor da seed parecido (fuzzy, `LIMIAR_AUTOR`) com algum autor
do item — aceita variação de grafia (Tolstoi × Tolstoy × Tolstoj). Item sem autor nunca confere.
Sem autor na seed, só a última tentativa, aceitando o primeiro item.

## Como executar

```bash
uv run python -m pipelines.livros.google_books.google_books_etl
uv run python -m pipelines.livros.google_books.google_books_etl --steps transform,load
```

A chave vem de `GOOGLE_BOOKS_API_KEY` no `.env` (`GoogleBooksSettings`, carregada no extract).

## Armadilhas

- **Incremental por livro**: o landing guarda um JSON por `<título>__<autor>` normalizados, e o
  extract só busca o que não tem JSON. Corrigir título ou autor na seed gera outra chave e,
  portanto, nova busca. Para refazer uma busca sem mudar a seed, apague o JSON.
- **Só a seed atual vai para a tabela**: o extract grava as chaves da seed em
  `parameters/chaves_seed.csv`, e o transform lê só os JSONs delas. Rodar `--steps transform`
  sem o extract usa a lista da última extração.
- **Sem item conferido não é gravado**: fica pendente e é buscado de novo a cada execução, com
  WARNING (não falha o extract). A API às vezes responde vazio para o que acha na chamada seguinte;
  o que nunca acha costuma ser grafia do título ou do autor na seed, ou livro fora do catálogo.
- Mesmo com o autor conferido, o item pode ser outra edição (outro idioma, outra editora).
- A chave precisa ter a **Books API** liberada nas restrições do Google Cloud; senão a API responde
  403 `API_KEY_SERVICE_BLOCKED`.
