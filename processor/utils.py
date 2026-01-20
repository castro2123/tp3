import aiohttp
import asyncio
import pandas as pd
from math import ceil
import os

# Chave da API FMP
FMP_API_KEY = os.getenv("FMP_API_KEY")  # definir no .env
API_CACHE = {}

async def get_financial_sentiment(ticker: str):
    """
    Consulta Financial Modeling Prep para pegar MarketCap, Sector, Industry, PERatio.
    """
    if not ticker or pd.isna(ticker):
        return {
            "Ticker": ticker,
            "MarketCap": None,
            "Sector": None,
            "Industry": None,
            "PERatio": None
        }

    if ticker in API_CACHE:
        return API_CACHE[ticker]

    url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    raise Exception(f"FMP API error {resp.status}")

                data = await resp.json()
                if not data or len(data) == 0:
                    result = {}
                else:
                    result = data[0]

                sentiment = {
                    "Ticker": ticker,
                    "MarketCap": result.get("mktCap"),
                    "Sector": result.get("sector"),
                    "Industry": result.get("industry"),
                    "PERatio": result.get("pe")
                }

                API_CACHE[ticker] = sentiment
                return sentiment

    except Exception as e:
        print(f"[API] Erro {ticker}: {e}")
        return {
            "Ticker": ticker,
            "MarketCap": None,
            "Sector": None,
            "Industry": None,
            "PERatio": None
        }

async def enrich_chunk(chunk: pd.DataFrame, batch_size: int = 20, batch_delay: float = 0.5):
    """
    Enriquecimento de um chunk de tickers em batches para evitar rate-limit.
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

        # Pequena pausa entre batches
        await asyncio.sleep(batch_delay)

    return pd.DataFrame(results)
