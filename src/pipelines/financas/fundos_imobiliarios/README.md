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
