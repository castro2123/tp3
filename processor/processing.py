import os
import io
import pandas as pd
import asyncio
from mapper import map_dataframe
from utils import enrich_chunk
from config import PROCESSED_PATH

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
