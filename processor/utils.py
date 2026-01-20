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

async def enrich_chunk(chunk, batch_size=100):
    """
    Enriquecimento assíncrono de um chunk do CSV.
    chunk: DataFrame com coluna 'Ticker'
    batch_size: quantos tickers processar por batch (yfinance.download suporta até ~200)
    Retorna lista de dicionários com os dados enriquecidos
    """

    tickers = chunk["Ticker"].tolist()
    enriched_rows = []

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        print(f"[ENRICH] Processando batch {i} a {i+len(batch)-1}")

        # Evita chamadas duplicadas para tickers já no cache
        tickers_to_download = [t for t in batch if t not in API_CACHE]

        batch_data = {}
        if tickers_to_download:
            try:
                # Download rápido via yfinance.download
                data = yf.download(
                    tickers=tickers_to_download,
                    period="1d",
                    group_by='ticker',
                    threads=True,
                    progress=False
                )
            except Exception as e:
                print(f"[WARNING] Erro no batch {i}-{i+len(batch)-1}: {e}")
                data = pd.DataFrame()

            # Popular cache com dados do batch
            for ticker in tickers_to_download:
                try:
                    info = yf.Ticker(ticker).info
                    API_CACHE[ticker] = {
                        "ticker": ticker,
                        "MarketCap": info.get("marketCap"),
                        "Sector": info.get("sector"),
                        "Industry": info.get("industry"),
                        "PERatio": info.get("trailingPE")
                    }
                except Exception:
                    # Se não existir ou erro, grava None
                    API_CACHE[ticker] = {
                        "ticker": ticker,
                        "MarketCap": None,
                        "Sector": None,
                        "Industry": None,
                        "PERatio": None
                    }

        # Montar resultado final para cada ticker do batch
        for ticker in batch:
            enriched_rows.append(API_CACHE.get(ticker, {
                "ticker": ticker,
                "MarketCap": None,
                "Sector": None,
                "Industry": None,
                "PERatio": None
            }))

        # Pequena pausa para evitar rate limit do Yahoo Finance
        await asyncio.sleep(0.5)

    return enriched_rows