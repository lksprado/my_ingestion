import argparse
import logging
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd
import yfinance as yf
from selenium import webdriver

from src.extractor import Extractor
from src.parser import parse_inv10_fund_table, parse_inv10_rankings_table
from src.utils.log import logger


def resolve_month(month_arg: str | None) -> tuple[str, str]:
    if month_arg:
        year, month = month_arg.split("-")
        return year, month
    now = datetime.now()
    return str(now.year), f"{now.month:02d}"


def investidor_10_data(year: str, month: str) -> Path:
    output_path = Path(f"data/{year}/{month}/fii_list.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    extraction_month = f"{year}-{month}"

    extract = Extractor()
    rows = []
    page = 1
    while True:
        url = f"https://investidor10.com.br/fiis/?page={page}"
        resp = extract.make_request(url=url, mode="text")
        if not resp:
            break
        context = parse_inv10_rankings_table(resp)
        if not context:
            break
        rows.extend(context)
        page += 1

    df = pd.DataFrame(rows)
    df.insert(0, "extraction_month", extraction_month)
    df.to_csv(output_path, index=False)
    logger.info(f"FII list saved: {output_path} ({len(df)} funds, {page - 1} pages)")
    return output_path


def investidor_10_details(tickers: list, year: str, month: str) -> tuple[int, int]:
    output_path = Path(f"data/{year}/{month}/indicadores/")
    output_path.mkdir(parents=True, exist_ok=True)
    extraction_month = f"{year}-{month}"

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=options)

    success, failed = 0, 0
    try:
        for ticker in tickers:
            try:
                url = f"https://investidor10.com.br/fiis/{ticker.lower()}/"
                driver.get(url)
                time.sleep(3)
                context = parse_inv10_fund_table(driver.page_source)
                if not context:
                    logger.warning(f"{ticker}: no data parsed")
                    failed += 1
                    continue
                df = pd.DataFrame(context)
                df.insert(0, "extraction_month", extraction_month)
                df.insert(1, "ticker", ticker)
                df.to_csv(output_path / f"{ticker}.csv", index=False)
                success += 1
                logger.info(f"✓ {ticker}")
            except Exception as e:
                logger.error(f"✗ {ticker}: {e}")
                failed += 1
    finally:
        driver.quit()

    return success, failed


def get_fii_history(
    tickers: list,
    year: str,
    month: str,
    start: str = "2020-01-01",
) -> Path:
    output_path = Path(f"data/{year}/{month}/fii_history.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    consolidated_rows = []

    for ticker in tickers:
        try:
            t = yf.Ticker(f"{ticker}.SA")
            hist = t.history(start=start)
            close_df = (
                hist[["Close"]].rename(columns={"Close": "close"})
                if not hist.empty
                else pd.DataFrame(columns=["close"])
            )
            div = t.dividends
            dividend_df = (
                div.to_frame(name="dividend")
                if not div.empty
                else pd.DataFrame(columns=["dividend"])
            )
            merged = close_df.join(dividend_df, how="outer")
            if merged.empty:
                logger.warning(f"{ticker}: no yfinance data")
                continue
            merged = merged.reset_index().rename(
                columns={"Date": "date", "index": "date"}
            )
            merged["ticker"] = ticker
            consolidated_rows.append(merged[["ticker", "date", "close", "dividend"]])
            logger.info(f"✓ {ticker}")
        except Exception as e:
            logger.error(f"✗ {ticker}: {e}")

    if consolidated_rows:
        result = (
            pd.concat(consolidated_rows, ignore_index=True)
            .sort_values(["ticker", "date"])
            .reset_index(drop=True)
        )
    else:
        result = pd.DataFrame(columns=["ticker", "date", "close", "dividend"])

    result.to_csv(output_path, index=False)
    logger.info(f"Price history saved: {output_path}")
    return output_path


def consolidate_fii_list(year: str, month: str, force: bool = False) -> None:
    source = Path(f"data/{year}/{month}/fii_list.csv")
    if not source.exists():
        logger.warning(f"consolidate_fii_list: {source} not found, skipping")
        return

    target = Path("data/consolidated/fii_list_history.csv")
    target.parent.mkdir(parents=True, exist_ok=True)
    extraction_month = f"{year}-{month}"

    new_df = pd.read_csv(source)

    if target.exists():
        existing = pd.read_csv(target)
        if extraction_month in existing["extraction_month"].values and not force:
            logger.info(
                f"consolidate_fii_list: {extraction_month} already present, "
                "skipping (use --force to overwrite)"
            )
            return
        combined = pd.concat(
            [existing[existing["extraction_month"] != extraction_month], new_df],
            ignore_index=True,
        )
    else:
        combined = new_df

    combined.to_csv(target, index=False)
    logger.info(f"Consolidated FII list updated: {target} ({len(combined)} rows)")


def consolidate_indicadores(year: str, month: str, force: bool = False) -> None:
    source_dir = Path(f"data/{year}/{month}/indicadores/")
    if not source_dir.exists():
        logger.warning(f"consolidate_indicadores: {source_dir} not found, skipping")
        return

    target = Path("data/consolidated/indicadores_history.csv")
    target.parent.mkdir(parents=True, exist_ok=True)
    extraction_month = f"{year}-{month}"

    ticker_files = list(source_dir.glob("*.csv"))
    if not ticker_files:
        logger.warning(f"consolidate_indicadores: no CSV files in {source_dir}")
        return

    new_df = pd.concat([pd.read_csv(f) for f in ticker_files], ignore_index=True)

    if target.exists():
        existing = pd.read_csv(target)
        if extraction_month in existing["extraction_month"].values and not force:
            logger.info(
                f"consolidate_indicadores: {extraction_month} already present, "
                "skipping (use --force to overwrite)"
            )
            return
        combined = pd.concat(
            [existing[existing["extraction_month"] != extraction_month], new_df],
            ignore_index=True,
        )
    else:
        combined = new_df

    combined.to_csv(target, index=False)
    logger.info(f"Consolidated indicadores updated: {target} ({len(combined)} rows)")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="FII monthly data pipeline")
    arg_parser.add_argument(
        "--month",
        help="Month to process in YYYY-MM format (default: current month)",
    )
    arg_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing month data and re-consolidate",
    )
    arg_parser.add_argument(
        "--consolidate-only",
        action="store_true",
        help="Skip extraction, only run consolidation",
    )
    args = arg_parser.parse_args()

    year, month = resolve_month(args.month)
    extraction_month = f"{year}-{month}"

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    file_handler = RotatingFileHandler(
        log_dir / f"{extraction_month}.log",
        maxBytes=10_000_000,
        backupCount=3,
    )
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | %(funcName)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(file_handler)

    logger.info(f"Pipeline started — month: {extraction_month}")
    start_time = datetime.now()

    fii_success, fii_failed = 0, 0

    if not args.consolidate_only:
        fii_list_path = Path(f"data/{year}/{month}/fii_list.csv")

        if fii_list_path.exists() and not args.force:
            logger.info(
                f"Extraction skipped: {fii_list_path} already exists "
                "(use --force to re-extract)"
            )
        else:
            investidor_10_data(year=year, month=month)

        tickers = pd.read_csv(fii_list_path)["ticker"].dropna().tolist()

        fii_success, fii_failed = investidor_10_details(
            tickers=tickers, year=year, month=month
        )

        get_fii_history(tickers=tickers, year=year, month=month)

    consolidate_fii_list(year=year, month=month, force=args.force)
    consolidate_indicadores(year=year, month=month, force=args.force)

    elapsed = str(datetime.now() - start_time).split(".")[0]
    logger.info(
        f"Pipeline complete — {fii_success} funds OK, "
        f"{fii_failed} failed — elapsed {elapsed}"
    )
