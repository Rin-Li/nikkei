import asyncio
import logging
import re
from datetime import date
from pathlib import Path

import pandas as pd
from tqdm.asyncio import tqdm_asyncio

from src.stock_fetcher import fetch_all


OUTPUT_DIR = Path("output")
COMPANIES_DIR = OUTPUT_DIR / "companies"
INDEX_PATH = OUTPUT_DIR / "index.md"


def find_latest_csv() -> Path:
    csvs = sorted(OUTPUT_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not csvs:
        raise FileNotFoundError("No CSV found in output/. Run main.py first.")
    return csvs[0]


def company_filename(stock_code: str, company_name: str) -> str:
    safe = re.sub(r'[\\/:*?"<>|]', "_", company_name)
    return f"{stock_code}_{safe}.md"


def format_market_cap(cap) -> str:
    if cap is None:
        return "N/A"
    if cap >= 1e12:
        return f"¥{cap/1e12:.1f}T"
    if cap >= 1e8:
        return f"¥{cap/1e8:.1f}億"
    return f"¥{cap:,.0f}"


def fmt(val, prefix="", suffix="", decimals=2, fallback="N/A"):
    if val is None:
        return fallback
    if isinstance(val, float):
        return f"{prefix}{val:,.{decimals}f}{suffix}"
    return f"{prefix}{val}{suffix}"


def fmt_sign(val, suffix="%", fallback="N/A"):
    if val is None:
        return fallback
    return f"{val:+.1f}{suffix}"


def append_stock_section(md_path: Path, stock: dict):
    if not md_path.exists():
        return

    existing = md_path.read_text(encoding="utf-8")

    marker = "## 株価データ"
    if marker in existing:
        return

    today = date.today().isoformat()
    cur = stock.get("currency", "JPY")
    sym = "¥" if cur == "JPY" else cur + " "

    rating_map = {"buy": "买入", "hold": "持有", "sell": "卖出", "strong_buy": "强烈买入", "underperform": "表现不佳"}
    rating_raw = stock.get("analyst_rating", "")
    rating = rating_map.get(rating_raw, rating_raw or "N/A")

    section = f"""
## 株価データ（{today}）

### 价格
| 项目 | 内容 |
|------|------|
| 现价 | {fmt(stock.get('price'), sym, '', 0)} |
| 昨收 | {fmt(stock.get('prev_close'), sym, '', 0)} |
| 今日区间 | {stock.get('day_range') or 'N/A'} |
| 52周高/低 | {fmt(stock.get('week52_high'), sym, '', 0)} / {fmt(stock.get('week52_low'), sym, '', 0)} |
| MA50 | {fmt(stock.get('ma50'), sym, '', 0)} |
| MA200 | {fmt(stock.get('ma200'), sym, '', 0)} |
| 近一年涨跌 | {fmt_sign(stock.get('change_1y'))} |
| 市值 | {format_market_cap(stock.get('market_cap'))} |

### 估值
| 项目 | 内容 |
|------|------|
| PE（TTM）| {fmt(stock.get('pe_ttm'), decimals=1)} |
| PE（Forward）| {fmt(stock.get('pe_forward'), decimals=1)} |
| PB | {fmt(stock.get('pb'), decimals=2)} |
| PS | {fmt(stock.get('ps'), decimals=2)} |
| EV/EBITDA | {fmt(stock.get('ev_ebitda'), decimals=1)} |
| PEG | {fmt(stock.get('peg'), decimals=2)} |

### 股息
| 项目 | 内容 |
|------|------|
| 股息率 | {fmt(stock.get('dividend_yield'), suffix='%', decimals=2)} |
| 每股股息 | {fmt(stock.get('dividend_rate'), sym, '', 1)} |
| 派息比率 | {fmt(stock.get('payout_ratio'), suffix='%', decimals=1)} |

### 盈利能力
| 项目 | 内容 |
|------|------|
| 毛利率 | {fmt(stock.get('gross_margin'), suffix='%')} |
| 营业利润率 | {fmt(stock.get('operating_margin'), suffix='%')} |
| 净利率 | {fmt(stock.get('net_margin'), suffix='%')} |
| ROE | {fmt(stock.get('roe'), suffix='%')} |
| ROA | {fmt(stock.get('roa'), suffix='%')} |

### 成长性
| 项目 | 内容 |
|------|------|
| 营收增长（YoY）| {fmt_sign(stock.get('revenue_growth'))} |
| 净利增长（YoY）| {fmt_sign(stock.get('earnings_growth'))} |

### 财务健康
| 项目 | 内容 |
|------|------|
| 流动比率 | {fmt(stock.get('current_ratio'), decimals=2)} |
| 速动比率 | {fmt(stock.get('quick_ratio'), decimals=2)} |
| 现金 | {format_market_cap(stock.get('total_cash'))} |
| 总负债 | {format_market_cap(stock.get('total_debt'))} |

### 分析师
| 项目 | 内容 |
|------|------|
| 评级 | {rating}（{fmt(stock.get('analyst_mean'), decimals=1)} / 5，{stock.get('analyst_count') or 'N/A'} 位分析师）|
| 目标价（均值）| {fmt(stock.get('target_mean'), sym, '', 0)} |
| 目标价区间 | {fmt(stock.get('target_low'), sym, '', 0)} — {fmt(stock.get('target_high'), sym, '', 0)} |
"""

    biz = stock.get("business_summary")
    if biz:
        biz_section = f"\n**Yahoo Finance:** {biz}\n"
        existing = existing.replace("## 分析摘要\n", f"## 分析摘要\n{biz_section}")

    backlink = "[← 返回 Index](../index.md)"
    if backlink in existing:
        updated = existing.replace(backlink, section + backlink)
    else:
        updated = existing + section

    md_path.write_text(updated, encoding="utf-8")


def rewrite_index(df_all: pd.DataFrame, stock_map: dict[str, dict]):
    total = len(df_all)
    today = date.today().isoformat()

    lines = [
        "# AI/半導体サプライチェーン スコアインデックス",
        "",
        f"- 分析済み: **{total}** 社",
        f"- 株価データ更新: {today}",
        "",
    ]

    df_sorted = df_all.sort_values("score", ascending=False)

    for score in range(10, 0, -1):
        group = df_sorted[df_sorted["score"] == score]
        if group.empty:
            continue

        if score >= 7:
            label = f"🔴 Score {score}"
        elif score >= 4:
            label = f"🟡 Score {score}"
        else:
            label = f"⚪ Score {score}"

        lines += [f"## {label} — {len(group)} 社", ""]

        for _, row in group.iterrows():
            fname = company_filename(row["stock_code"], row["company_name"])
            link = f"companies/{fname}"
            role = row.get("role", "-")

            stock = stock_map.get(str(row["stock_code"]))
            if stock and score >= 4:
                price_str = f"¥{stock['price']:,.0f}" if stock.get("price") else ""
                change_str = f"{stock['change_1y']:+.1f}%" if stock.get("change_1y") is not None else ""
                pe_str = f"PE {stock['pe_ttm']:.1f}" if stock.get("pe_ttm") else ""
                fpe_str = f"fPE {stock['pe_forward']:.1f}" if stock.get("pe_forward") else ""
                parts = [p for p in [price_str, change_str, pe_str, fpe_str] if p]
                suffix = f" | {' · '.join(parts)}" if parts else ""
            else:
                suffix = ""

            lines.append(f"- [{row['company_name']} ({row['stock_code']})]({link}) — {role}{suffix}")

        lines.append("")

    INDEX_PATH.write_text("\n".join(lines), encoding="utf-8")


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    csv_path = find_latest_csv()
    logging.info(f"Loading {csv_path}")
    df = pd.read_csv(csv_path, dtype={"stock_code": str})

    ai_df = df[df["score"] >= 4].copy()
    logging.info(f"Fetching stock data for {len(ai_df)} AI-related companies...")

    codes = ai_df["stock_code"].tolist()
    results = await tqdm_asyncio.gather(
        *[fetch_all([c]) for c in codes],
        desc="Fetching stock data",
    )
    flat_results = [r[0] for r in results]

    stock_map = {r["stock_code"]: r for r in flat_results if "error" not in r}
    error_count = sum(1 for r in flat_results if "error" in r)
    logging.info(f"Fetched {len(stock_map)} OK, {error_count} errors")

    for _, row in ai_df.iterrows():
        stock = stock_map.get(str(row["stock_code"]))
        if not stock:
            continue
        fname = company_filename(row["stock_code"], row["company_name"])
        md_path = COMPANIES_DIR / fname
        append_stock_section(md_path, stock)

    rewrite_index(df, stock_map)
    logging.info("Done. index.md updated.")


if __name__ == "__main__":
    asyncio.run(main())
