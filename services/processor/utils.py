import os
import re
import ssl
import json
from datetime import datetime, timezone
import aiohttp
import asyncio
import pandas as pd
from dotenv import load_dotenv

try:
    import certifi
except ImportError:
    certifi = None

def _load_env_files():
    paths = [".env", os.path.join("env", "tp3.env"), os.path.join("env", "tp3-1.env")]
    loaded = False
    for path in paths:
        if os.path.exists(path):
            load_dotenv(path, override=True)
            loaded = True
    if not loaded:
        load_dotenv()

_load_env_files()

SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where()) if certifi else None

# Variáveis da FMP API
FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_API_BASE = os.getenv("FMP_API_BASE", "https://financialmodelingprep.com/stable").rstrip("/")
FMP_RESOLVE_ISIN = os.getenv("FMP_RESOLVE_ISIN", "0").lower() in ("1", "true", "yes")
FMP_DAILY_LIMIT = int(os.getenv("FMP_DAILY_LIMIT", "250"))
FMP_CACHE_PATH = os.getenv("FMP_CACHE_PATH", "data/cache/fmp_cache.json")
ENRICH_MAX_TICKERS = int(os.getenv("ENRICH_MAX_TICKERS", "20"))
ENRICH_TOTAL_MAX = int(os.getenv("ENRICH_TOTAL_MAX", "0"))
if not FMP_API_KEY:
    raise RuntimeError("FMP_API_KEY não definida. Configure a variável de ambiente.")

API_CACHE = {}
ISIN_CACHE = {}
ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
LINK_ISIN_RE = re.compile(r"/([A-Z]{2}[A-Z0-9]{10})(?:-|$)", re.IGNORECASE)
_ENRICH_REMAINING = ENRICH_TOTAL_MAX
_ENRICH_LOCK = None
_CACHE_LOCK = None
_CACHE_LOADED = False
_REQUEST_COUNT = 0
_CACHE_DATE = None
_LIMIT_WARNED = False

def _today_key():
    return datetime.now(timezone.utc).date().isoformat()

def _ensure_cache_loaded():
    global _CACHE_LOADED, _REQUEST_COUNT, _CACHE_DATE
    if _CACHE_LOADED:
        return
    data = {}
    try:
        if FMP_CACHE_PATH and os.path.exists(FMP_CACHE_PATH):
            with open(FMP_CACHE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
    except Exception:
        data = {}

    API_CACHE.update(data.get("api", {}))
    ISIN_CACHE.update(data.get("isin", {}))

    _CACHE_DATE = data.get("date") or _today_key()
    _REQUEST_COUNT = int(data.get("count") or 0)
    if _CACHE_DATE != _today_key():
        _CACHE_DATE = _today_key()
        _REQUEST_COUNT = 0
    _CACHE_LOADED = True

def _save_cache():
    if not FMP_CACHE_PATH:
        return
    os.makedirs(os.path.dirname(FMP_CACHE_PATH), exist_ok=True)
    payload = {
        "date": _CACHE_DATE,
        "count": _REQUEST_COUNT,
        "api": API_CACHE,
        "isin": ISIN_CACHE,
    }
    with open(FMP_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2, sort_keys=True)

def _get_cache_lock():
    global _CACHE_LOCK
    if _CACHE_LOCK is None:
        _CACHE_LOCK = asyncio.Lock()
    return _CACHE_LOCK

def _reset_daily_count_if_needed():
    global _CACHE_DATE, _REQUEST_COUNT
    today = _today_key()
    if _CACHE_DATE != today:
        _CACHE_DATE = today
        _REQUEST_COUNT = 0

def _warn_limit_once():
    global _LIMIT_WARNED
    if not _LIMIT_WARNED:
        print("[API] Limite diario atingido. Enriquecimento suspenso ate amanha.")
        _LIMIT_WARNED = True

async def _reserve_request_slot():
    if FMP_DAILY_LIMIT <= 0:
        return True
    _ensure_cache_loaded()
    async with _get_cache_lock():
        global _REQUEST_COUNT
        _reset_daily_count_if_needed()
        if _REQUEST_COUNT >= FMP_DAILY_LIMIT:
            _warn_limit_once()
            return False
        _REQUEST_COUNT += 1
        _save_cache()
        return True

async def _update_api_cache(symbol, sentiment):
    _ensure_cache_loaded()
    async with _get_cache_lock():
        API_CACHE[symbol] = sentiment
        _save_cache()

async def _update_isin_cache(isin, symbol):
    _ensure_cache_loaded()
    async with _get_cache_lock():
        ISIN_CACHE[isin] = symbol
        _save_cache()

async def get_financial_sentiment(session, ticker: str, retries: int = 3, delay: float = 0.5):
    """
    Pega dados financeiros de um ticker usando FMP API.
    Trata erros 403, 429 e outros de forma segura.
    """
    _ensure_cache_loaded()
    if not ticker or pd.isna(ticker):
        return {"MarketCap": None, "Sector": None, "Industry": None, "PERatio": None}

    symbol = _normalize_symbol(str(ticker))
    if ISIN_RE.match(symbol):
        if not FMP_RESOLVE_ISIN:
            return {"MarketCap": None, "Sector": None, "Industry": None, "PERatio": None}
        symbol = await resolve_isin_to_symbol(session, symbol)
        if not symbol:
            return {"MarketCap": None, "Sector": None, "Industry": None, "PERatio": None}
    if not symbol:
        return {"MarketCap": None, "Sector": None, "Industry": None, "PERatio": None}

    # Usa cache se já consultado
    if symbol in API_CACHE:
        return API_CACHE[symbol]

    url = f"{FMP_API_BASE}/profile?symbol={symbol}&apikey={FMP_API_KEY}"

    for attempt in range(1, retries + 1):
        try:
            if not await _reserve_request_slot():
                return {"MarketCap": None, "Sector": None, "Industry": None, "PERatio": None}
            async with session.get(url, timeout=10, ssl=SSL_CONTEXT) as resp:
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
                        await _update_api_cache(symbol, sentiment)
                        return sentiment

                    elif resp.status == 403:
                        # Key inválida ou endpoint proibido
                        print(f"[API] Erro 403 para {symbol}: API key inválida ou sem permissão")
                        return {"MarketCap": None, "Sector": None, "Industry": None, "PERatio": None}

                    elif resp.status == 429:
                        # Rate-limit, espera antes de tentar de novo
                        print(f"[API] Erro 429 (rate-limit) para {symbol}, tentativa {attempt}")
                        await asyncio.sleep(delay * attempt)
                        continue

                    else:
                        print(f"[API] Erro {resp.status} para {symbol}, tentativa {attempt}")
                        await asyncio.sleep(delay * attempt)
                        continue

        except asyncio.TimeoutError:
            print(f"[API] Timeout para {symbol}, tentativa {attempt}")
            await asyncio.sleep(delay * attempt)
        except Exception as e:
            print(f"[API] Exceção para {symbol}: {e}, tentativa {attempt}")
            await asyncio.sleep(delay * attempt)

    # Se falhar todas as tentativas
    return {"MarketCap": None, "Sector": None, "Industry": None, "PERatio": None}


async def enrich_chunk(chunk: pd.DataFrame, batch_size: int = 20, batch_delay: float = 0.05):
    """
    Enriquecimento assíncrono de um DataFrame de tickers.
    Processa em batches para evitar rate-limit.
    """
    results = [
        {"MarketCap": None, "Sector": None, "Industry": None, "PERatio": None}
        for _ in range(len(chunk))
    ]

    tickers = chunk["Ticker"].tolist()
    links = chunk["Link"].tolist() if "Link" in chunk.columns else [None] * len(chunk)
    batch_size = int(os.getenv("ENRICH_BATCH_SIZE", str(batch_size)))
    batch_delay = float(os.getenv("ENRICH_BATCH_DELAY", str(batch_delay)))
    max_tickers = int(os.getenv("ENRICH_MAX_TICKERS", str(ENRICH_MAX_TICKERS)))

    eligible = []
    for idx, ticker in enumerate(tickers):
        if len(eligible) >= max_tickers:
            continue
        symbol = str(ticker).strip().upper() if ticker and not pd.isna(ticker) else ""
        symbol = _normalize_symbol(symbol) if symbol else ""
        link_isin = _extract_isin_from_link(links[idx]) if FMP_RESOLVE_ISIN else None

        if FMP_RESOLVE_ISIN and link_isin:
            if not symbol or not symbol.isalpha() or len(symbol) > 5:
                symbol = link_isin

        if not symbol:
            continue
        if ISIN_RE.match(symbol) and not FMP_RESOLVE_ISIN:
            continue
        eligible.append((idx, symbol))

    symbol_to_indices = {}
    for idx, symbol in eligible:
        symbol_to_indices.setdefault(symbol, []).append(idx)

    reserved = await _reserve_slots([(0, symbol) for symbol in symbol_to_indices.keys()])
    reserved_symbols = [symbol for _, symbol in reserved]

    async with aiohttp.ClientSession() as session:
        n_batches = (len(reserved_symbols) + batch_size - 1) // batch_size
        for i in range(n_batches):
            batch = reserved_symbols[i * batch_size:(i + 1) * batch_size]
            tasks = [get_financial_sentiment(session, symbol) for symbol in batch]
            batch_results = await asyncio.gather(*tasks)
            for symbol, payload in zip(batch, batch_results):
                for idx in symbol_to_indices.get(symbol, []):
                    results[idx] = payload
            await asyncio.sleep(batch_delay)  # evita rate-limit

    return pd.DataFrame(results)

async def resolve_isin_to_symbol(session, isin: str):
    _ensure_cache_loaded()
    if isin in ISIN_CACHE:
        return ISIN_CACHE[isin]
    url = f"{FMP_API_BASE}/search-symbol?query={isin}&apikey={FMP_API_KEY}"
    try:
        if not await _reserve_request_slot():
            return None
        async with session.get(url, timeout=10, ssl=SSL_CONTEXT) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            if isinstance(data, list) and data:
                symbol = data[0].get("symbol")
                if symbol:
                    await _update_isin_cache(isin, symbol)
                    return symbol
    except Exception:
        return None
    return None

def _normalize_symbol(value: str) -> str:
    symbol = value.strip().upper()
    symbol = re.sub(r"^[0-9]+", "", symbol)
    return symbol

def _extract_isin_from_link(value: str):
    if not value or pd.isna(value):
        return None
    match = LINK_ISIN_RE.search(str(value))
    if not match:
        return None
    return match.group(1).upper()

async def _reserve_slots(eligible):
    if ENRICH_TOTAL_MAX <= 0:
        return eligible
    global _ENRICH_REMAINING, _ENRICH_LOCK
    if _ENRICH_LOCK is None:
        _ENRICH_LOCK = asyncio.Lock()
    async with _ENRICH_LOCK:
        if _ENRICH_REMAINING <= 0:
            return []
        take = min(len(eligible), _ENRICH_REMAINING)
        _ENRICH_REMAINING -= take
        return eligible[:take]
