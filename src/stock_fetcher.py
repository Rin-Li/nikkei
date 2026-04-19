import asyncio
import logging

import yfinance as yf


async def fetch_one(stock_code: str) -> dict:
    def _call():
        ticker = yf.Ticker(f"{stock_code}.T")
        info = ticker.info

        hist = ticker.history(period="1y")
        change_1y = None
        if len(hist) >= 2:
            change_1y = round((hist["Close"].iloc[-1] / hist["Close"].iloc[0] - 1) * 100, 2)

        def get(key):
            v = info.get(key)
            return v if v not in (None, "", "N/A", 0.0) or key in ("totalDebt",) else v

        def pct(key):
            v = info.get(key)
            return round(v * 100, 2) if v is not None else None

        return {
            "stock_code": stock_code,
            # 价格
            "price": info.get("currentPrice"),
            "prev_close": info.get("previousClose"),
            "day_range": info.get("regularMarketDayRange"),
            "week52_high": info.get("fiftyTwoWeekHigh"),
            "week52_low": info.get("fiftyTwoWeekLow"),
            "ma50": info.get("fiftyDayAverage"),
            "ma200": info.get("twoHundredDayAverage"),
            "change_1y": change_1y,
            "market_cap": info.get("marketCap"),
            "currency": info.get("currency", "JPY"),
            # 估值
            "pe_ttm": info.get("trailingPE"),
            "pe_forward": info.get("forwardPE"),
            "pb": info.get("priceToBook"),
            "ps": info.get("priceToSalesTrailing12Months"),
            "ev_ebitda": info.get("enterpriseToEbitda"),
            "peg": info.get("pegRatio"),
            # 股息
            "dividend_yield": pct("dividendYield"),
            "dividend_rate": info.get("dividendRate"),
            "payout_ratio": pct("payoutRatio"),
            # 盈利能力
            "gross_margin": pct("grossMargins"),
            "operating_margin": pct("operatingMargins"),
            "net_margin": pct("profitMargins"),
            "roe": pct("returnOnEquity"),
            "roa": pct("returnOnAssets"),
            # 成长性
            "revenue_growth": pct("revenueGrowth"),
            "earnings_growth": pct("earningsGrowth"),
            # 财务健康
            "current_ratio": info.get("currentRatio"),
            "quick_ratio": info.get("quickRatio"),
            "total_cash": info.get("totalCash"),
            "total_debt": info.get("totalDebt"),
            # 公司介绍
            "business_summary": info.get("longBusinessSummary"),
            # 分析师
            "analyst_rating": info.get("recommendationKey"),
            "analyst_mean": info.get("recommendationMean"),
            "analyst_count": info.get("numberOfAnalystOpinions"),
            "target_mean": info.get("targetMeanPrice"),
            "target_high": info.get("targetHighPrice"),
            "target_low": info.get("targetLowPrice"),
        }

    try:
        return await asyncio.get_event_loop().run_in_executor(None, _call)
    except Exception as e:
        logging.warning(f"Stock fetch failed for {stock_code}: {e}")
        return {"stock_code": stock_code, "error": str(e)}


async def fetch_all(stock_codes: list[str], max_concurrent: int = 10) -> list[dict]:
    sem = asyncio.Semaphore(max_concurrent)

    async def _guarded(code):
        async with sem:
            return await fetch_one(code)

    return await asyncio.gather(*[_guarded(c) for c in stock_codes])
