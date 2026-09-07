"""
Relatório de dividendos projetados com aporte de R$10.000.
Lê o arquivo consolidado mais recente e gera CSV + HTML.
"""

import re
from pathlib import Path

import pandas as pd

INVESTMENT = 10_000.0
SOURCE = Path("data/consolidated/fii_list_history.csv")
OUTPUT_DIR = Path("reports")


def parse_percent(value: str) -> float | None:
    if not value or value.strip() == "-":
        return None
    cleaned = value.replace("%", "").replace(",", ".").strip()
    try:
        return float(cleaned) / 100
    except ValueError:
        return None


def parse_liquidity(value: str) -> float | None:
    """Converts '20,91 M' → 20_910_000  |  '876,50 K' → 876_500."""
    if not value or value.strip() == "-":
        return None
    value = value.replace(",", ".").strip()
    match = re.match(r"([\d.]+)\s*([MKB]?)", value, re.IGNORECASE)
    if not match:
        return None
    num = float(match.group(1))
    suffix = match.group(2).upper()
    multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(suffix, 1)
    return num * multiplier


def build_report(month: str, df_month: pd.DataFrame) -> pd.DataFrame:
    df = df_month.copy()

    df["dy_12m_pct"] = df["dividend_yield_last_12_months"].apply(parse_percent)
    df["dy_5y_pct"] = df["dividend_yield_last_5_years"].apply(parse_percent)
    df["liquidity_brl"] = df["daily_liquidity"].apply(parse_liquidity)

    df = df[df["dy_12m_pct"].notna()].copy()

    df["dividendo_anual_brl"] = (df["dy_12m_pct"] * INVESTMENT).round(2)
    df["dividendo_mensal_brl"] = (df["dividendo_anual_brl"] / 12).round(2)
    df["dy_12m_pct_display"] = (df["dy_12m_pct"] * 100).round(2)
    df["dy_5y_pct_display"] = (df["dy_5y_pct"] * 100).round(2)

    result = df[[
        "ticker",
        "fii_type",
        "name_segment",
        "dy_12m_pct_display",
        "dy_5y_pct_display",
        "dividendo_anual_brl",
        "dividendo_mensal_brl",
        "liquidity_brl",
        "p_vp",
    ]].rename(columns={
        "ticker": "Ticker",
        "fii_type": "Tipo",
        "name_segment": "Segmento",
        "dy_12m_pct_display": "DY 12m (%)",
        "dy_5y_pct_display": "DY 5a (%)",
        "dividendo_anual_brl": "Dividendo Anual (R$)",
        "dividendo_mensal_brl": "Dividendo Mensal (R$)",
        "liquidity_brl": "Liquidez Diária (R$)",
        "p_vp": "P/VP",
    }).sort_values("DY 12m (%)", ascending=False).reset_index(drop=True)

    return result


def to_html(df: pd.DataFrame, month: str) -> str:
    rows_html = ""
    for _, row in df.iterrows():
        liq = row["Liquidez Diária (R$)"]
        if pd.notna(liq) and liq >= 1_000_000:
            liq_str = f"R$ {liq/1_000_000:.2f} M"
        elif pd.notna(liq) and liq >= 1_000:
            liq_str = f"R$ {liq/1_000:.2f} K"
        elif pd.notna(liq):
            liq_str = f"R$ {liq:.0f}"
        else:
            liq_str = "-"

        dy5 = f"{row['DY 5a (%)']:.2f}%" if pd.notna(row["DY 5a (%)"]) else "-"

        rows_html += f"""
        <tr>
          <td><strong>{row['Ticker']}</strong></td>
          <td>{row['Tipo']}</td>
          <td>{row['Segmento']}</td>
          <td class="num">{row['DY 12m (%)']:.2f}%</td>
          <td class="num">{dy5}</td>
          <td class="num">R$ {row['Dividendo Anual (R$)']:,.2f}</td>
          <td class="num highlight">R$ {row['Dividendo Mensal (R$)']:,.2f}</td>
          <td class="num">{liq_str}</td>
          <td class="num">{row['P/VP']}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8"/>
  <title>Relatório de Dividendos FII — {month}</title>
  <style>
    body {{ font-family: 'Segoe UI', sans-serif; margin: 2rem; color: #1a1a2e; background: #f8f9fa; }}
    h1 {{ color: #16213e; }}
    .subtitle {{ color: #555; margin-bottom: 1.5rem; font-size: 0.95rem; }}
    table {{ border-collapse: collapse; width: 100%; background: #fff; box-shadow: 0 2px 8px rgba(0,0,0,.08); border-radius: 8px; overflow: hidden; }}
    th {{ background: #16213e; color: #fff; padding: 10px 14px; text-align: left; font-size: 0.85rem; }}
    td {{ padding: 8px 14px; border-bottom: 1px solid #eee; font-size: 0.875rem; }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: #f0f4ff; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .highlight {{ color: #0f7d3c; font-weight: 700; }}
    .summary {{ display: flex; gap: 2rem; margin-bottom: 1.5rem; flex-wrap: wrap; }}
    .card {{ background: #fff; border-radius: 8px; padding: 1rem 1.5rem; box-shadow: 0 2px 8px rgba(0,0,0,.08); min-width: 160px; }}
    .card-label {{ font-size: 0.75rem; color: #888; text-transform: uppercase; letter-spacing: .05em; }}
    .card-value {{ font-size: 1.5rem; font-weight: 700; color: #16213e; }}
    .card-value.green {{ color: #0f7d3c; }}
  </style>
</head>
<body>
  <h1>Relatório de Dividendos — FIIs</h1>
  <p class="subtitle">Referência: {month} &nbsp;|&nbsp; Aporte simulado: <strong>R$ {INVESTMENT:,.0f}</strong> &nbsp;|&nbsp; Fonte: Investidor 10</p>

  <div class="summary">
    <div class="card">
      <div class="card-label">Fundos analisados</div>
      <div class="card-value">{len(df)}</div>
    </div>
    <div class="card">
      <div class="card-label">DY médio (12m)</div>
      <div class="card-value">{df['DY 12m (%)'].mean():.2f}%</div>
    </div>
    <div class="card">
      <div class="card-label">Dividendo mensal médio</div>
      <div class="card-value green">R$ {df['Dividendo Mensal (R$)'].mean():.2f}</div>
    </div>
    <div class="card">
      <div class="card-label">Maior DY (12m)</div>
      <div class="card-value green">{df['DY 12m (%)'].max():.2f}%</div>
    </div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Ticker</th><th>Tipo</th><th>Segmento</th>
        <th>DY 12m</th><th>DY 5a</th>
        <th>Dividendo Anual</th><th>Dividendo Mensal</th>
        <th>Liquidez Diária</th><th>P/VP</th>
      </tr>
    </thead>
    <tbody>{rows_html}
    </tbody>
  </table>
</body>
</html>"""


def main():
    df_all = pd.read_csv(SOURCE)
    latest_month = df_all["extraction_month"].max()
    df_month = df_all[df_all["extraction_month"] == latest_month]

    report = build_report(latest_month, df_month)

    csv_path = OUTPUT_DIR / f"dividend_report_{latest_month}.csv"
    report.to_csv(csv_path, index=False)
    print(f"CSV salvo: {csv_path}")

    html_path = OUTPUT_DIR / f"dividend_report_{latest_month}.html"
    html_path.write_text(to_html(report, latest_month), encoding="utf-8")
    print(f"HTML salvo: {html_path}")

    print(f"\nAporte: R$ {INVESTMENT:,.0f} | Mês: {latest_month} | Fundos: {len(report)}")
    print(f"DY médio: {report['DY 12m (%)'].mean():.2f}% | "
          f"Dividendo mensal médio: R$ {report['Dividendo Mensal (R$)'].mean():.2f}")
    print(f"\nTop 10 por DY 12m:")
    print(report[[
        "Ticker", "DY 12m (%)", "Dividendo Mensal (R$)", "Tipo", "P/VP"
    ]].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
