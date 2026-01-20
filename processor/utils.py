import aiohttp
import asyncio
import pandas as pd
import os
import io
from mapper import map_dataframe
from config import PROCESSED_PATH

API_CACHE = {}

async def get_financial_sentiment(ticker, session):
    if not ticker or pd.isna(ticker):
        return {
            "MarketCap": None,
            "Sector": None,
            "Industry": None,
            "PERatio": None
        }

    if ticker in API_CACHE:
        return API_CACHE[ticker]

    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=summaryDetail,assetProfile"

    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200:
                raise Exception(f"Yahoo API error {resp.status}")

            data = await resp.json()
            result = data.get("quoteSummary", {}).get("result", [{}])[0]

            sentiment = {
                "MarketCap": result.get("summaryDetail", {}).get("marketCap", {}).get("raw"),
                "Sector": result.get("assetProfile", {}).get("sector"),
                "Industry": result.get("assetProfile", {}).get("industry"),
                "PERatio": result.get("summaryDetail", {}).get("trailingPE", {}).get("raw")
            }

            API_CACHE[ticker] = sentiment
            return sentiment

    except Exception as e:
        print(f"[API] Erro {ticker}: {e}")
        return {
            "MarketCap": None,
            "Sector": None,
            "Industry": None,
            "PERatio": None
        }


async def enrich_chunk(chunk, batch_size=20, batch_delay=0.05):
    """
    Enriquecimento assíncrono de um chunk de tickers
    """
    tickers = chunk["Ticker"].tolist()
    results = []

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            tasks = [get_financial_sentiment(ticker, session) for ticker in batch]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
            await asyncio.sleep(batch_delay)  # evita rate-limit

    return pd.DataFrame(results)