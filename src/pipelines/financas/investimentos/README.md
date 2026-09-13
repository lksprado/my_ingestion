# Pipeline: Investimentos

Ingestão das posições de investimento pessoais a partir de quatro fontes de formatos
bem diferentes (Excel, PDF, Google Sheets e o próprio DW), landando CSVs nos schemas
`raw_b3`, `raw_avenue` e `raw_google` do Postgres. Aqui é **só ingestão**: a
categorização e a análise vivem no dbt. Configuração em `investimentos_config.yml`
(`load: files`: cada CSV do bronze vira a tabela de mesmo nome).

A intervenção nos dados é mínima — o suficiente para torná-los tabulares com nomes
de coluna limpos. Por isso as cargas são **full refresh**: o volume é pequeno e a
idempotência vem de recarregar tudo, o que também tolera mudança de schema na origem.

## Fontes

| Script | Source | Origem | Entrada | Tabelas destino |
|---|---|---|---|---|
| `investimentos_b3.py` | `b3` | B3 | Excel mensal (uma aba por classe de ativo), já no landing | `raw_b3.<aba>` |
| `investimentos_avenue.py` | `avenue` | Avenue | PDF de Account Statement, já no landing | `raw_avenue.assets`, `raw_avenue.dividends_interest` |
| `investimentos_google.py` | `google` | Google Sheets | Abas declaradas em `options.sheets` | `raw_google.<aba>` (e `raw_google.<aba>_<workbook>` nas secundárias) |
| `investimentos_fgc.py` | `fgc` | DW + CSV do Bacen | `intermediate.int_renda_fixa` | `de_para_instituicoes_fgc.csv` (seed do dbt) — **exceção**, sem GenericETL |

## Como executar

```bash
uv run python -m pipelines.financas.investimentos.run_all          # b3 + avenue + google
uv run python -m pipelines.financas.investimentos.run_all b3       # uma fonte só
uv run python -m pipelines.financas.investimentos.investimentos_google --steps transform,load
uv run python -m pipelines.financas.investimentos.investimentos_fgc  # à parte (ver abaixo)
```

O orquestrador (`core.run_many`) roda as três ingestões em sequência e **não aborta**
quando uma falha: registra o erro, segue para a próxima e sai com código 1 no fim.
O `fgc` fica fora porque depende da camada `intermediate` já materializada no DW.

## Pré-requisitos

- **`pdftotext`** (poppler-utils) instalado no sistema — o avenue chama o binário
  via `subprocess`. Sem ele, essa fonte falha.
- **`GOOGLE_CREDENTIALS_FILE`** no `.env`, apontando para o JSON da service account
  (o arquivo fica fora do repo, em `~/.secrets/`).
- **`URL_FINANCE__<CHAVE>`** no `.env` (delimitador duplo), uma por workbook de
  `options.sheets` (`URL_FINANCE__LUCAS_JESSICA`, `URL_FINANCE__DEUSA`); chega em
  `settings.url_finance`. Workbook sem URL é pulado com log.

## Entradas esperadas no lake

```
${LAKE_ROOT}/raw/investments/b3/<pessoa>/*.xlsx
${LAKE_ROOT}/raw/investments/avenue/<pessoa>/<current|legacy>/*.pdf
${LAKE_ROOT}/raw/investments/google/<workbook>_<aba>.json      # gerado pelo extract
${LAKE_ROOT}/raw/investments/instituicoes/instituicoes_conglomerado_prudencial.csv
```

O bronze é gravado em `${LAKE_ROOT}/bronze/investments/<fonte>/<tabela>.csv` (`;`).

O transform **apaga os CSVs do bronze antes de regravar** (`_common.reset_bronze`):
com `load: files` qualquer arquivo que sobrasse viraria uma tabela fantasma.

## Detalhes por fonte

**B3** — lê cada Excel, explode todas as abas, agrupa por nome normalizado da aba
(aplicando `ALIASES` e removendo o prefixo `posicao_`) e concatena tudo em
`<nome>.csv`. `source_path` guarda o caminho absoluto do Excel de origem, que já
carrega a pessoa no path.

**Avenue** — parte frágil do conjunto: converte o PDF com `pdftotext -layout` e
varre a tabela de posições linha a linha com heurísticas de regex. A pessoa e o
layout (`current`/`legacy`) vêm do caminho. Inclui uma validação que compara a soma
de `market_value` por classe com os totais declarados no PDF e loga `MISMATCH`
acima de US$ 0,05 — vale olhar esses warnings antes de confiar na carga.

**Google Sheets** — o extract grava `get_all_values()` de cada aba como JSON no
landing; o transform aplica `header_row` (índice 0-based da linha de cabeçalho),
normaliza o cabeçalho e grava o bronze. Adicionar uma aba é uma linha em
`options.sheets`. O **primeiro** workbook é o primário e mantém nomes "limpos"
(`raw_google.patrimonio`); os demais recebem o sufixo da chave
(`raw_google.patrimonio_deusa`).

**FGC** — lê os emissores distintos de `intermediate.int_renda_fixa` (só produtos
cobertos pelo FGC: CDB/LCA/LCI/LC) via `PostgresClient.read_sql`, faz fuzzy match
com `rapidfuzz` contra a lista de conglomerados prudenciais e herda o conglomerado.
Matches abaixo do threshold (80) saem como warning. A saída vai para `SEEDS_ROOT`,
virando seed do dbt.

## Convenções

- Colunas de rastreio: `source_path`/`source_file` guardam o caminho absoluto do
  arquivo bruto; o Google usa `source_sheet` e `source_workbook`.
