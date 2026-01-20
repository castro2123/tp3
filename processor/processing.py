import os
import io
import pandas as pd
import asyncio
from mapper import map_dataframe
from utils import enrich_chunk
from config import PROCESSED_PATH

async def process_chunk(chunk, batch_size=20, batch_delay=0.05):
    chunk_mapped = map_dataframe(chunk)
    financial_df = await enrich_chunk(chunk_mapped, batch_size=batch_size, batch_delay=batch_delay)
    chunk_enriched = pd.concat([chunk_mapped.reset_index(drop=True), financial_df.reset_index(drop=True)], axis=1)
    return chunk_enriched


async def process_csv_stream_async(content, chunk_size=200, batch_size=20, batch_delay=0.05):
    print("[PROCESSOR] Processando CSV em stream assíncrono...")
    os.makedirs(os.path.dirname(PROCESSED_PATH), exist_ok=True)

    chunks = [chunk for chunk in pd.read_csv(io.BytesIO(content), chunksize=chunk_size)]
    tasks = [process_chunk(chunk, batch_size=batch_size, batch_delay=batch_delay) for chunk in chunks]
    results = await asyncio.gather(*tasks)

    for i, chunk_enriched in enumerate(results):
        chunk_enriched.to_csv(
            PROCESSED_PATH,
            mode="w" if i == 0 else "a",
            header=(i == 0),
            index=False,
            encoding="utf-8-sig"
        )

    print(f"[PROCESSOR] CSV processado salvo em {PROCESSED_PATH}")
    return PROCESSED_PATH