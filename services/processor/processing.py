import os
import io
import hashlib
import pandas as pd
import asyncio
from mapper import map_dataframe
from utils import enrich_chunk
from config import PROCESSED_PATH

DEMO_MODE = os.getenv("DEMO_MODE", "0").lower() in ("1", "true", "yes")
DEMO_LAST_PRICE = os.getenv("DEMO_LAST_PRICE", "EUR 10.00")
DEMO_SECTOR = os.getenv("DEMO_SECTOR", "Technology")
DEMO_INDUSTRY = os.getenv("DEMO_INDUSTRY", "Software")
DEMO_MARKET_CAP = os.getenv("DEMO_MARKET_CAP", "1000000000")
DEMO_PE_RATIO = os.getenv("DEMO_PE_RATIO", "18.5")
DEMO_SECTORS = [
    item.strip()
    for item in os.getenv(
        "DEMO_SECTORS",
        "Technology,Healthcare,Finance,Energy,Consumer,Industrial,Utilities,Materials",
    ).split(",")
    if item.strip()
]
DEMO_INDUSTRIES = {
    "Technology": ["Software", "Semiconductors", "IT Services"],
    "Healthcare": ["Pharma", "Biotech", "Medical Devices"],
    "Finance": ["Banking", "Insurance", "Asset Management"],
    "Energy": ["Oil & Gas", "Renewables", "Utilities Services"],
    "Consumer": ["Retail", "Food & Beverage", "Leisure"],
    "Industrial": ["Manufacturing", "Logistics", "Aerospace"],
    "Utilities": ["Power", "Water", "Gas"],
    "Materials": ["Chemicals", "Metals", "Construction"],
}

def _is_missing(value):
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    text = str(value).strip()
    return text == "" or text in {"-", "--", "nan", "None"}

def _is_placeholder(value, placeholder):
    if value is None:
        return False
    return str(value).strip() == str(placeholder).strip()

def _demo_seed(row):
    key = f"{row.get('Ticker','')}-{row.get('Nome','')}"
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)

def _demo_pick(options, seed, offset=0):
    if not options:
        return None
    return options[(seed + offset) % len(options)]

def _demo_market_cap(seed):
    cap = 500_000_000 + (seed % 900) * 25_000_000
    return str(int(cap))

def _demo_pe_ratio(seed):
    pe = 8 + (seed % 260) / 10
    return f"{pe:.1f}"

def _demo_currency(mercado):
    code = str(mercado or "").upper()
    if code in {"XOSL", "MERK", "XOAS"}:
        return "NOK"
    return "EUR"

def _demo_price(seed, mercado):
    price = 5 + (seed % 5000) / 100
    text = f"{price:.2f}".replace(".", ",")
    return f"{_demo_currency(mercado)} {text}"

def apply_demo_defaults(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche apenas campos vazios para modo demonstracao.
    """
    updates = 0
    for idx, row in df.iterrows():
        seed = _demo_seed(row)

        if "Último_Preço" in df.columns and (
            _is_missing(row.get("Último_Preço"))
            or _is_placeholder(row.get("Último_Preço"), DEMO_LAST_PRICE)
        ):
            df.at[idx, "Último_Preço"] = _demo_price(seed, row.get("Mercado"))
            updates += 1

        if "Sector" in df.columns and (
            _is_missing(row.get("Sector"))
            or _is_placeholder(row.get("Sector"), DEMO_SECTOR)
        ):
            sector = _demo_pick(DEMO_SECTORS, seed) or DEMO_SECTOR
            df.at[idx, "Sector"] = sector
            updates += 1
        else:
            sector = row.get("Sector")

        if "Industry" in df.columns and (
            _is_missing(row.get("Industry"))
            or _is_placeholder(row.get("Industry"), DEMO_INDUSTRY)
        ):
            sector_key = str(sector or DEMO_SECTOR)
            industries = DEMO_INDUSTRIES.get(sector_key, [DEMO_INDUSTRY])
            df.at[idx, "Industry"] = _demo_pick(industries, seed, offset=3) or DEMO_INDUSTRY
            updates += 1

        if "MarketCap" in df.columns and (
            _is_missing(row.get("MarketCap"))
            or _is_placeholder(row.get("MarketCap"), DEMO_MARKET_CAP)
        ):
            df.at[idx, "MarketCap"] = _demo_market_cap(seed)
            updates += 1

        if "PERatio" in df.columns and (
            _is_missing(row.get("PERatio"))
            or _is_placeholder(row.get("PERatio"), DEMO_PE_RATIO)
        ):
            df.at[idx, "PERatio"] = _demo_pe_ratio(seed)
            updates += 1

    return df

async def process_chunk(chunk, batch_size=20, batch_delay=0.05):
    """
    Processa um chunk de CSV: mapeia colunas e enriquece via API externa.
    """
    try:
        # Mapeia colunas do chunk para o schema padrão
        chunk_mapped = map_dataframe(chunk)

        # Verifica se existe coluna "Ticker"
        if "Ticker" not in chunk_mapped.columns:
            print("[PROCESSOR] WARNING: coluna 'Ticker' não encontrada neste chunk")
            chunk_mapped["Ticker"] = None

        # Enriquecimento financeiro via API externa
        financial_df = await enrich_chunk(
            chunk_mapped,
            batch_size=batch_size,
            batch_delay=batch_delay
        )

        # Concatenar dados originais + enriquecidos
        chunk_enriched = pd.concat(
            [chunk_mapped.reset_index(drop=True), financial_df.reset_index(drop=True)],
            axis=1
        )

        if DEMO_MODE:
            chunk_enriched = apply_demo_defaults(chunk_enriched)

        return chunk_enriched

    except Exception as e:
        print(f"[PROCESSOR] Erro ao processar chunk: {e}")
        return pd.DataFrame()  # Retorna dataframe vazio para não quebrar o loop


async def process_csv_stream_async(content, chunk_size=200, batch_size=20, batch_delay=0.05):
    """
    Processa um CSV completo em chunks assíncronos e salva no caminho PROCESSED_PATH.
    """
    print("[PROCESSOR] Processando CSV em stream assíncrono...")
    os.makedirs(os.path.dirname(PROCESSED_PATH), exist_ok=True)

    # Lê CSV em chunks
    try:
        chunks = [chunk for chunk in pd.read_csv(io.BytesIO(content), chunksize=chunk_size)]
    except Exception as e:
        print(f"[PROCESSOR] Erro ao ler CSV: {e}")
        return None

    # Cria tarefas assíncronas para cada chunk
    tasks = [
        process_chunk(chunk, batch_size=batch_size, batch_delay=batch_delay)
        for chunk in chunks
    ]

    # Executa todos os chunks em paralelo
    results = await asyncio.gather(*tasks)

    # Salva CSV final
    for i, chunk_enriched in enumerate(results):
        if chunk_enriched.empty:
            continue
        chunk_enriched.to_csv(
            PROCESSED_PATH,
            mode="w" if i == 0 else "a",
            header=(i == 0),
            index=False,
            encoding="utf-8-sig"
        )

    print(f"[PROCESSOR] CSV processado salvo em {PROCESSED_PATH}")
    return PROCESSED_PATH
