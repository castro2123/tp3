import os
import aiohttp
import asyncio
import pandas as pd

# Variável de ambiente da FMP API
FMP_API_KEY = os.getenv("FMP_API_KEY")
if not FMP_API_KEY:
    raise RuntimeError("FMP_API_KEY não definida. Configure a variável de ambiente.")

API_CACHE = {}

async def get_financial_sentiment(ticker: str, retries: int = 3, delay: float = 0.5):
    """
    Pega dados financeiros de um ticker usando FMP API.
    Trata erros 403, 429 e outros de forma segura.
    """
    if not ticker or pd.isna(ticker):
        return {"MarketCap": None, "Sector": None, "Industry": None, "PERatio": None}

    # Usa cache se já consultado
    if ticker in API_CACHE:
        return API_CACHE[ticker]

    url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"

    for attempt in range(1, retries + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if not data:
                            raise ValueError("Resposta vazia da API")

                        profile = data[0]
                        sentiment = {
                            "MarketCap": profile.get("mktCap"),
                            "Sector": profile.get("sector"),
                            "Industry": profile.get("industry"),
                            "PERatio": profile.get("trailingPE")
                        }
                        API_CACHE[ticker] = sentiment
                        return sentiment

                    elif resp.status == 403:
                        # Key inválida ou endpoint proibido
                        print(f"[API] Erro 403 para {ticker}: API key inválida ou sem permissão")
                        return {"MarketCap": None, "Sector": None, "Industry": None, "PERatio": None}

                    elif resp.status == 429:
                        # Rate-limit, espera antes de tentar de novo
                        print(f"[API] Erro 429 (rate-limit) para {ticker}, tentativa {attempt}")
                        await asyncio.sleep(delay * attempt)
                        continue

                    else:
                        print(f"[API] Erro {resp.status} para {ticker}, tentativa {attempt}")
                        await asyncio.sleep(delay * attempt)
                        continue

        except asyncio.TimeoutError:
            print(f"[API] Timeout para {ticker}, tentativa {attempt}")
            await asyncio.sleep(delay * attempt)
        except Exception as e:
            print(f"[API] Exceção para {ticker}: {e}, tentativa {attempt}")
            await asyncio.sleep(delay * attempt)

    # Se falhar todas as tentativas
    return {"MarketCap": None, "Sector": None, "Industry": None, "PERatio": None}


async def enrich_chunk(chunk: pd.DataFrame, batch_size: int = 20, batch_delay: float = 0.05):
    """
    Enriquecimento assíncrono de um DataFrame de tickers.
    Processa em batches para evitar rate-limit.
    """
    results = []
    tickers = chunk["Ticker"].tolist()
    n_batches = (len(tickers) + batch_size - 1) // batch_size

    for i in range(n_batches):
        batch = tickers[i * batch_size:(i + 1) * batch_size]
        tasks = [get_financial_sentiment(t) for t in batch]
        batch_results = await asyncio.gather(*tasks)
        results.extend(batch_results)
        await asyncio.sleep(batch_delay)  # evita rate-limit

    return pd.DataFrame(results)
