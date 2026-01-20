from grpc import aio
from config import GRPC_SERVICE_ADDR
from processing_hints_pb2 import HintsRequest
from processing_hints_pb2_grpc import ProcessingHintsStub

DEFAULT_HINTS = {
    "chunk_size": 200,
    "batch_size": 20,
    "batch_delay": 0.05,
}

async def fetch_processing_hints(source="euronext"):
    if not GRPC_SERVICE_ADDR:
        return DEFAULT_HINTS
    try:
        async with aio.insecure_channel(GRPC_SERVICE_ADDR) as channel:
            stub = ProcessingHintsStub(channel)
            response = await stub.GetHints(HintsRequest(source=source), timeout=5)
            return {
                "chunk_size": response.chunk_size or DEFAULT_HINTS["chunk_size"],
                "batch_size": response.batch_size or DEFAULT_HINTS["batch_size"],
                "batch_delay": response.batch_delay or DEFAULT_HINTS["batch_delay"],
            }
    except Exception as exc:
        print(f"[gRPC] Falha ao obter hints: {exc}")
        return DEFAULT_HINTS
