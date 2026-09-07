from src.parser import parse_inv10_fund_table, parse_inv10_rankings_table

RANKINGS_HTML = """
<html><body>
<table id="rankigns">
  <tbody>
    <tr>
      <td><a href="/fiis/kncr11/"><span class="font-semibold">KNCR11</span></a></td>
      <td data-name="p_vp">1,03</td>
      <td data-name="dividend_yield_last_12_months">13,92%</td>
    </tr>
    <tr>
      <td><a href="/fiis/xpml11/"><span class="font-semibold">XPML11</span></a></td>
      <td data-name="p_vp">0,95</td>
      <td data-name="dividend_yield_last_12_months">10,50%</td>
    </tr>
  </tbody>
</table>
</body></html>
"""

FUND_TABLE_HTML = """
<html><body>
<table id="table-indicators-history">
  <tbody>
    <tr>
      <th></th>
      <th class="year">Atual</th>
      <th class="year">2025</th>
      <th class="year">2024</th>
    </tr>
    <tr>
      <td class="indicator">P/VP</td>
      <td class="value">1,03</td>
      <td class="value">1,05</td>
      <td class="value">1,01</td>
    </tr>
    <tr>
      <td class="indicator">Dividend Yield</td>
      <td class="value">13,92%</td>
      <td class="value">11,50%</td>
      <td class="value">10,20%</td>
    </tr>
  </tbody>
</table>
</body></html>
"""


def test_rankings_table_returns_tickers():
    rows = parse_inv10_rankings_table(RANKINGS_HTML)
    assert len(rows) == 2
    assert rows[0]["ticker"] == "KNCR11"
    assert rows[1]["ticker"] == "XPML11"


def test_rankings_table_returns_named_columns():
    rows = parse_inv10_rankings_table(RANKINGS_HTML)
    assert rows[0]["p_vp"] == "1,03"
    assert rows[0]["dividend_yield_last_12_months"] == "13,92%"


def test_rankings_table_empty_on_missing_table():
    assert parse_inv10_rankings_table("<html><body></body></html>") == []


def test_rankings_table_empty_on_garbage_html():
    assert parse_inv10_rankings_table("not html at all") == []


def test_fund_table_returns_indicators():
    rows = parse_inv10_fund_table(FUND_TABLE_HTML)
    assert len(rows) == 2
    indicators = [r["indicator"] for r in rows]
    assert "P/VP" in indicators
    assert "Dividend Yield" in indicators


def test_fund_table_maps_year_columns():
    rows = parse_inv10_fund_table(FUND_TABLE_HTML)
    pvp = next(r for r in rows if r["indicator"] == "P/VP")
    assert pvp["Atual"] == "1,03"
    assert pvp["2025"] == "1,05"
    assert pvp["2024"] == "1,01"


def test_fund_table_empty_on_missing_table():
    assert parse_inv10_fund_table("<html><body></body></html>") == []


def test_fund_table_empty_on_garbage_html():
    assert parse_inv10_fund_table("not html at all") == []
