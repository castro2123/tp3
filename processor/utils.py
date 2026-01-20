import time
import pandas as pd
import yfinance as yf

API_CACHE = {}

def get_financial_sentiment(ticker, retries=3):
    if ticker in API_CACHE:
        return API_CACHE[ticker]

    for attempt in range(retries):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            data = {
                "MarketCap": info.get("marketCap"),
                "Sector": info.get("sector"),
                "Industry": info.get("industry"),
                "PERatio": info.get("trailingPE")
            }
            API_CACHE[ticker] = data
            return data
        except Exception as e:
            print(f"[API] Erro {ticker} tentativa {attempt+1}: {e}")
            time.sleep(1)
    return {"MarketCap": None, "Sector": None, "Industry": None, "PERatio": None, "API_Error": True}

async def enrich_chunk(chunk):
    import asyncio
    loop = asyncio.get_event_loop()
    results = []
    for ticker in chunk["Ticker"]:
        if pd.isna(ticker):
            results.append({"ticker": None, "sentiment": None})
            continue
        try:
            # Chama função de blocking no executor para não bloquear loop
            sentiment = await loop.run_in_executor(None, get_financial_sentiment, ticker)
            results.append(sentiment)
        except Exception as e:
            print(f"[API] Erro {ticker}: {e}")
            results.append({"ticker": ticker, "sentiment": None})
        await asyncio.sleep(0.1)
    return results
