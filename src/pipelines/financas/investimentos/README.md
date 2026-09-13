# Pipeline: Investimentos

Ingestão das posições de investimento pessoais a partir de quatro fontes de formatos
bem diferentes (Excel, PDF, Google Sheets e o próprio DW), landando CSVs nos schemas
`raw_b3`, `raw_avenue` e `raw_google` do Postgres. Aqui é **só ingestão**: a categorização e a análise vivem no dbt.

A intervenção nos dados é mínima — o suficiente para torná-los tabulares com nomes
de coluna limpos. Por isso as cargas são **full refresh**: o volume é pequeno e a
idempotência vem de recarregar tudo, o que também tolera mudança de schema na origem.

## Fontes

| Módulo | Origem | Entrada | Tabelas destino |
|---|---|---|---|
| `b3_etl.py` | B3 | Excel mensal, uma aba por classe de ativo | `raw_b3.<aba>` (prefixo `consolidado_` removido) |
| `avenue_etl.py` | Avenue | PDF de Account Statement | `raw_avenue.assets`, `raw_avenue.dividends_interest` |
| `google_finance_etl.py` | Google Sheets | Planilhas declaradas em `config.yml` | `raw_google.<aba>` (e `raw_google.<aba>_<workbook>` nas secundárias) |
| `fgc_etl.py` | DW + CSV do Bacen | `intermediate.int_renda_fixa` | `de_para_instituicoes_fgc.csv` (seed do dbt) |

## Como executar

```bash
uv run python -m pipelines.financas.investimentos.run_all          # b3 + avenue + google
uv run python -m pipelines.financas.investimentos.run_all b3       # uma fonte só
uv run python -m pipelines.financas.investimentos.run_all fgc      # roda à parte (ver abaixo)
```

O orquestrador roda as três ingestões em sequência e **não aborta** quando uma
falha: registra o erro, segue para a próxima e sai com código 1 no fim se houve
falha. O `fgc` fica fora do fluxo padrão porque depende da camada `intermediate`
já materializada no DW.

## Pré-requisitos

- **`pdftotext`** (poppler-utils) instalado no sistema — o `avenue_etl` chama o
  binário via `subprocess`. Sem ele, essa fonte falha.
- **`GOOGLE_CREDENTIALS_FILE`** no `.env`, apontando para o JSON da service account
  (o arquivo fica fora do repo, em `~/.secrets/`).
- **`URL_FINANCE_<CHAVE>`** no `.env`, uma por workbook do `config.yml`
  (`URL_FINANCE_LUCAS_JESSICA`, `URL_FINANCE_DEUSA`). Workbook sem URL é pulado
  com log, sem abortar os demais.

## Entradas esperadas no lake

```
${LAKE_ROOT}/raw/investments/b3/                    # planilhas da B3
${LAKE_ROOT}/raw/investments/avenue/<pessoa>/<current|legacy>/*.pdf
${LAKE_ROOT}/raw/investments/instituicoes/instituicoes_conglomerado_prudencial.csv
```

O bronze intermediário é gravado em `${LAKE_ROOT}/bronze/investments/<fonte>/`.

## Detalhes por fonte

**B3** — lê cada Excel, explode todas as abas, agrupa por nome normalizado da aba
(aplicando `FILE_ALIASES` e removendo o prefixo `posicao_`) e concatena tudo num
`consolidado_<nome>.csv`. Não há CSV intermediário por mês. `source_path` guarda o
caminho absoluto do Excel de origem, que já carrega a pessoa no path.

**Avenue** — parte frágil do conjunto: converte o PDF com `pdftotext -layout` e
varre a tabela de posições linha a linha com heurísticas de regex. A pessoa e o
layout (`current`/`legacy`) vêm do caminho. Inclui uma validação que compara a soma
de `market_value` por classe com os totais declarados no PDF e loga `MISMATCH`
acima de US$ 0,05 — vale olhar esses warnings antes de confiar na carga.

**Google Sheets** — `config.yml` mapeia workbook → lista de abas, cada aba com seu
`header_row` (índice 0-based da linha de cabeçalho). Adicionar uma aba é mudança de
uma linha no config. O **primeiro** workbook do config é o primário e mantém nomes
"limpos" (`raw_google.patrimonio`); os demais recebem o sufixo da chave
(`raw_google.patrimonio_deusa`) — é assim que duas abas homônimas convivem.

**FGC** — lê os emissores distintos de `intermediate.int_renda_fixa` (só produtos
cobertos pelo FGC: CDB/LCA/LCI/LC), faz fuzzy match com `rapidfuzz` contra a lista
de conglomerados prudenciais e herda o conglomerado. A normalização remove sufixos
societários (S.A., LTDA, BANCO…) para não inflar o score; matches abaixo do
threshold (80) saem como warning. A saída vai para `SEEDS_ROOT`, virando seed do dbt.

## Convenções herdadas

- Funções que retornam devolvem um `path` ou string — nunca DataFrame.
- Colunas de rastreio: `source_path`/`source_file` guardam o caminho absoluto do
  arquivo bruto; o Google usa `source_sheet` e `source_workbook`.
