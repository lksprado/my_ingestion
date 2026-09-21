# Pipeline: Fundos Imobiliários (FII)

Coleta mensal de dados de FIIs do [Investidor 10](https://investidor10.com.br/fiis/)
e do Yahoo Finance, consolidando um histórico mês a mês. Diferente dos outros
pipelines de finanças, **não carrega no Postgres**: a saída são CSVs no lake e um
relatório de dividendos em HTML.

## Etapas

| Função (`run.py`) | O que faz | Saída |
|---|---|---|
| `investidor_10_data` | Pagina o ranking de FIIs via HTTP | `<mês>/fii_list.csv` |
| `investidor_10_details` | Selenium headless por ticker (indicadores) | `<mês>/indicadores/{TICKER}.csv` |
| `get_fii_history` | Preços e proventos via `yfinance` (`{ticker}.SA`) | `<mês>/fii_history.csv` |
| `consolidate_fii_list` | Acumula o histórico da lista | `consolidated/fii_list_history.csv` |
| `consolidate_indicadores` | Acumula o histórico de indicadores | `consolidated/indicadores_history.csv` |

Tudo sob `${LAKE_ROOT}/raw/fii/`, com `<mês>` = `{YYYY}/{MM}`.

## Colunas de saída

`fii_list.csv` (e o `consolidated/fii_list_history.csv`, que é a concatenação
mensal dele):

| Coluna | O que é |
|---|---|
| `extraction_month` | Mês de referência da extração (`YYYY-MM`) |
| `ticker` | Código do fundo (ex.: `KNCR11`) |
| `net_worth` | Patrimônio líquido |
| `p_vp` | Preço sobre valor patrimonial |
| `dividend_yield_last_12_months` | DY dos últimos 12 meses |
| `dividend_yield_last_5_years` | DY médio dos últimos 5 anos |
| `daily_liquidity` | Liquidez diária |
| `fii_type` | Tipo de fundo (Papel, Tijolo, …) |
| `variation_12_months` / `two_years_variation` / `variation_5_years` | Variação de preço no período |
| `name_segment` | Segmento |

Os nomes vêm do atributo `data-name` do HTML (`parse_inv10_rankings_table`), não
de uma lista fixa: coluna nova no site aparece sozinha no CSV.

`indicadores/{TICKER}.csv` (e `consolidated/indicadores_history.csv`):
`extraction_month`, `ticker`, `indicator` (P/VP, Dividend Yield, …), `Atual` e
uma coluna por ano disponível (`2025`, `2024`, …).

`fii_history.csv`: `ticker`, `date`, `close`, `dividend`.

## Como executar

```bash
uv run python -m pipelines.financas.fundos_imobiliarios.run                      # mês corrente
uv run python -m pipelines.financas.fundos_imobiliarios.run --month 2026-09      # mês específico
uv run python -m pipelines.financas.fundos_imobiliarios.run --force              # reextrai e sobrescreve
uv run python -m pipelines.financas.fundos_imobiliarios.run --consolidate-only   # só consolida
```

Por padrão a extração é **pulada** se o `fii_list.csv` do mês já existe — use
`--force` para refazer. A consolidação é idempotente: se o `extraction_month` já
está no histórico, ela não duplica (também respeita `--force`).

## Relatório de dividendos

```bash
uv run python -m pipelines.financas.fundos_imobiliarios.dividend_report
```

Lê `consolidated/fii_list_history.csv`, simula um aporte fixo (R$ 10.000) e gera
CSV + HTML em `${LAKE_ROOT}/reports/fii/`.

## Notas

- **Selenium com Chrome** é necessário para os indicadores (`--headless=new`); há
  um `sleep(3)` por ticker, então a etapa é lenta e proporcional ao número de FIIs.
  O driver é encerrado no `finally`, e falhas por ticker são contabilizadas sem
  abortar a execução.
- O seletor da tabela de rankings é `table#rankigns` — **o typo é do site**, não
  daqui. Se o parsing vier vazio, confira se corrigiram o HTML.
- Os logs do mês vão para `${LAKE_ROOT}/raw/fii/logs/{YYYY-MM}.log`
  (`core.setup_logger(log_file=...)`).
- Exceção consciente ao `GenericETL`: não escreve em `raw_*`; usa só `HttpClient`
  e `setup_logger` da `core`.
- Testes do parser em `tests/financas/test_fii_parser.py`.

## Limitações conhecidas

- `get_fii_history` é chamada sem `start` (`run.py:266`), então cai no default
  `"2020-01-01"` e **rebaixa a série inteira do yfinance a cada execução**, em vez
  de buscar só o incremento desde o último mês consolidado.
