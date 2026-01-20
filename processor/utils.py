import asyncio
import pandas as pd
import yfinance as yf
from collections import defaultdict
import time

# Cache local para evitar múltiplas chamadas
API_CACHE = {}

async def get_financial_sentiment(ticker, retries=5, delay=1.0):
    """
    Obtém dados financeiros de um ticker usando yfinance.
    Faz retries exponenciais e respeita rate limiting.
    """
    if not ticker or pd.isna(ticker):
        return {
            "ticker": ticker,
            "MarketCap": None,
            "Sector": None,
            "Industry": None,
            "PERatio": None,
            "API_Error": True
        }

    # Retorna do cache se já existirem dados
    if ticker in API_CACHE:
        return API_CACHE[ticker]

    for attempt in range(retries):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            # Algumas chaves podem não existir
            data = {
                "ticker": ticker,
                "MarketCap": info.get("marketCap"),
                "Sector": info.get("sector"),
                "Industry": info.get("industry"),
                "PERatio": info.get("trailingPE"),
                "API_Error": False
            }

            # Salva no cache
            API_CACHE[ticker] = data
            return data

        except Exception as e:
            wait_time = delay * (2 ** attempt)  # Retry exponencial
            print(f"[API] Erro {ticker} tentativa {attempt+1}: {e}. Tentando de novo em {wait_time:.1f}s")
            await asyncio.sleep(wait_time)

    # Se falhar todas as tentativas, retorna dados nulos
    return {
        "ticker": ticker,
        "MarketCap": None,
        "Sector": None,
        "Industry": None,
        "PERatio": None,
        "API_Error": True
    }

async def enrich_chunk(chunk, batch_delay=0.5):
    """
    Enriquecimento assíncrono de um chunk do CSV.
    Usa asyncio.gather para processar múltiplos tickers.
    Adiciona delay entre requisições para evitar rate limits.
    """
    results = []

    async def process_ticker(ticker):
        data = await get_financial_sentiment(ticker)
        await asyncio.sleep(batch_delay)  # Pequeno delay entre tickers
        return data

    tasks = [process_ticker(ticker) for ticker in chunk["Ticker"]]
    results = await asyncio.gather(*tasks)

    return results
