import aiohttp
import asyncio
import pandas as pd
from math import ceil
import os
FMP_API_KEY = os.getenv("FMP_API_KEY")


API_CACHE = {}

async def get_financial_sentiment(ticker: str):
    """
    Pega dados financeiros de um ticker usando FMP API.
    Retorna None se o ticker não existir.
    """
    if not ticker or pd.isna(ticker):
        return {
            "ticker": ticker,
            "MarketCap": None,
            "DividendYield": None,
            "Sector": None,
            "Industry": None,
        }

    if ticker in API_CACHE:
        return API_CACHE[ticker]

    url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    data = {
                        "ticker": ticker,
                        "MarketCap": None,
                        "DividendYield": None,
                        "Sector": None,
                        "Industry": None,
                    }
                else:
                    result = await resp.json()
                    if not result:
                        data = {
                            "ticker": ticker,
                            "MarketCap": None,
                            "DividendYield": None,
                            "Sector": None,
                            "Industry": None,
                        }
                    else:
                        profile = result[0]
                        data = {
                            "ticker": ticker,
                            "MarketCap": profile.get("mktCap"),
                            "DividendYield": profile.get("dividendYield"),
                            "Sector": profile.get("sector"),
                            "Industry": profile.get("industry"),
                        }
        API_CACHE[ticker] = data
        return data

    except Exception:
        return {
            "ticker": ticker,
            "MarketCap": None,
            "DividendYield": None,
            "Sector": None,
            "Industry": None,
        }


async def enrich_chunk(chunk: pd.DataFrame, batch_size: int = 50, batch_delay: float = 0.05):
    """
    Enriquecimento ultra-rápido de um chunk de CSV.
    Divide os tickers em batches de 'batch_size' e processa em paralelo.
    batch_delay adiciona atraso entre batches para evitar rate-limit.
    """
    tickers = chunk["Ticker"].tolist()
    n_batches = ceil(len(tickers) / batch_size)
    results = []

    for i in range(n_batches):
        batch = tickers[i * batch_size:(i + 1) * batch_size]

        async def process_batch(batch):
            tasks = [get_financial_sentiment(ticker) for ticker in batch]
            return await asyncio.gather(*tasks)

        batch_results = await process_batch(batch)
        results.extend(batch_results)

        await asyncio.sleep(batch_delay)

    return pd.DataFrame(results)
