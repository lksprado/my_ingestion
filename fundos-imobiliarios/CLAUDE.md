# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
uv sync --no-install-project   # install dependencies
uv run python main.py          # run pipeline
uv run pytest                  # run all tests
uv run pytest tests/test_foo.py::test_bar  # run single test
uv run ruff check .            # lint
uv run ruff fix .              # auto-fix lint issues
```

## Architecture

This is a monthly data collection pipeline for Brazilian Real Estate Investment Funds (FIIs) from investidor10.com.br. The full specification is in `docs/PRD.md`.

**Data flow:**
1. `investidor_10_data()` — HTTP scrape of listing pages → `data/.../fii_list.csv`
2. `investidor_10_details(tickers)` — Selenium scrape of per-fund detail pages → `data/.../indicadores/{TICKER}.csv`
3. `get_fii_history(tickers)` — yfinance pull of price/dividend history → `data/.../fii_history.csv`
4. Consolidation — monthly files appended into `data/consolidated/`

**Modules:**
- `src/extractor.py` — `Extractor` class: `requests.Session` with retry (backoff on 403/429/5xx). Call `make_request(url, mode="text"|"json"|"auto")`.
- `src/parser.py` — BeautifulSoup parsers. Note: the listings table selector is `table#rankigns` (typo in the site's HTML, not a bug here). `parse_inv10_fund_table` returns rows with an `indicator` key and one key per year column.
- `src/utils/log.py` — exports a pre-configured `logger` instance. Import with `from src.utils.log import logger`. Supports optional rotating file output via `setup_logger(log_to_file=True)`.

**Known issues to be aware of (tracked in PRD backlog):**
- `make_df()` in `main.py` references `f` outside the loop scope — broken.
- `investidor_10_details()` runs Selenium without headless mode and has no driver teardown.
- Page limit in `investidor_10_data()` is hardcoded to 4.
- Output paths are not month-aware yet; files have been renamed manually (`_abril`, `_maio`).

**Target output structure (per PRD, not yet implemented):**
```
data/YYYY/MM/fii_list.csv
data/YYYY/MM/fii_history.csv
data/YYYY/MM/indicadores/{TICKER}.csv
data/consolidated/fii_list_history.csv
data/consolidated/indicadores_history.csv
```
