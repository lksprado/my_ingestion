import re

from bs4 import BeautifulSoup


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def clean_text(text: str | None) -> str | None:
    if text is None:
        return None
    return " ".join(text.replace("\xa0", " ").split()).strip()


def parse_inv10_rankings_table(html: str):
    soup = BeautifulSoup(html, "html.parser")

    # Atenção: no HTML veio "rankigns" (com typo), não "rankings"
    table = soup.select_one("table#rankigns")
    if not table:
        return []

    rows = []
    for tr in table.select("tbody tr"):
        row = {}

        # Ticker (primeira coluna "Ativos")
        ticker_el = tr.select_one("td:first-child a span.font-semibold")
        row["ticker"] = clean(ticker_el.get_text()) if ticker_el else None

        # Demais colunas (já vêm com data-name)
        for td in tr.select("td[data-name]"):
            key = td.get("data-name")
            row[key] = clean(td.get_text(" ", strip=True))

        rows.append(row)

    return rows


def parse_inv10_fund_table(html: str):
    soup = BeautifulSoup(html, "html.parser")

    table = soup.select_one("table#table-indicators-history")
    if not table:
        return []

    rows = table.select("tbody tr")
    if not rows:
        return []

    # primeira linha = cabeçalho
    header_row = rows[0]
    headers = [clean_text(th.get_text()) for th in header_row.select("th.year")]

    results = []

    # demais linhas = indicadores
    for tr in rows[1:]:
        indicator_el = tr.select_one("td.indicator")
        if not indicator_el:
            continue

        indicator = clean(indicator_el.get_text(" ", strip=True))
        values = [clean(td.get_text(" ", strip=True)) for td in tr.select("td.value")]

        row = {"indicator": indicator}

        for year, value in zip(headers, values):
            row[year] = value

        results.append(row)

    return results


if __name__ == "__main__":
    from .extractor import Extractor

    extract = Extractor()
    # url = "https://investidor10.com.br/fiis/?page=1"

    # resp = extract.make_request(url=url, mode="text")

    # context = parse_inv10_rankings_table(resp)

    # print(context)
    # url = "https://investidor10.com.br/fiis/habt11/"
    # driver = webdriver.Chrome()
    # driver.get("https://investidor10.com.br/fiis/habt11/")

    # html = driver.page_source

    # soup = BeautifulSoup(html, "html.parser").prettify()

    # with open("fund_table.html", "w", encoding="utf-8") as f:
    #     f.write(soup)

    with open("fund_table.html", "r", encoding="utf-8") as f:
        html = f.read()

    import pandas as pd

    data = parse_inv10_fund_table(html)
    df = pd.DataFrame(data)
    print(df)
