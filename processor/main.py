import os
import io
import asyncio
import aiohttp
import pandas as pd
import uuid
import threading
from utils import enrich_chunk  # função de enriquecimento que usa yfinance
from mapper import map_dataframe
from webhook import app as webhook_app

# ----------------------
# Configurações Supabase
# ----------------------
BUCKET_NAME = "data"
FILE_NAME = "Crawler/euronext_acoes.csv"
PROCESSED_PATH = "data/Processed/acoes_enriched.csv"

WEBHOOK_XML_URL = os.getenv("XML_SERVICE_URL")
JAVA_WEBHOOK_URL = os.getenv("JAVA_WEBHOOK_URL")
PROCESSOR_WEBHOOK_PORT = int(os.getenv("PROCESSOR_WEBHOOK_PORT", 5000))
MAPPER_VERSION = os.getenv('MAPPER_VERSION', '1.0')

PENDING_REQUESTS = {}

# ----------------------
# Config Supabase
# ----------------------
def load_env(filename=".env"):
    env = {}
    current_dir = os.path.abspath(os.getcwd())
    parent_dir = os.path.dirname(current_dir)

    possible_paths = [
        os.path.join(current_dir, filename),
        os.path.join(parent_dir, filename),
    ]

    env_path = None
    for path in possible_paths:
        if os.path.exists(path):
            env_path = path
            break

    if not env_path:
        print("Aviso: ficheiro .env não encontrado.")
        return env

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):]
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip("'\"")

    return env


def validate_supabase_config(url, legacy_key, service_key, legacy_key_source):
    errors, warnings = [], []
    if not url:
        errors.append("URL ausente no .env (variavel URL).")
    if not legacy_key:
        errors.append("LEGACY_KEY ausente no .env (ou KEY).")
    if url and not url.startswith("https://"):
        warnings.append("URL nao comeca com https://.")
    if url and "supabase.co" not in url:
        warnings.append("URL nao parece ser do Supabase.")
    if legacy_key_source == "KEY":
        warnings.append("LEGACY_KEY nao definido; usando KEY como legacy.")
    if service_key:
        warnings.append("SERVICE_KEY definido, mas o modo legacy usa apenas LEGACY_KEY/KEY.")
    return errors, warnings

def get_supabase_config():
    env = load_env()
    url = env.get("URL") or os.getenv("URL")
    key = env.get("LEGACY_KEY") or os.getenv("LEGACY_KEY") or os.getenv("KEY")
    return url, key

# ----------------------
# Download CSV assíncrono
# ----------------------
async def download_from_bucket_async(session, bucket_name, file_name):
    url, key = get_supabase_config()
    if not url or not key:
        print("[ERROR] Configuração Supabase inválida.")
        return None

    storage_url = f"{url}/storage/v1/object/{bucket_name}/{file_name}"
    headers = {"Authorization": f"Bearer {key}", "apikey": key}

    async with session.get(storage_url, headers=headers) as resp:
        if resp.status == 404:
            return None
        if resp.status in (400, 401, 403):
            storage_url = f"{url}/storage/v1/object/public/{bucket_name}/{file_name}"
            async with session.get(storage_url) as resp_public:
                if resp_public.status != 200:
                    print(f"[ERROR] Download público falhou: {resp_public.status}")
                    return None
                return await resp_public.read()
        if resp.status != 200:
            print(f"[ERROR] Download falhou: {resp.status}")
            return None
        return await resp.read()

# ----------------------
# Polling assíncrono do bucket
# ----------------------
async def poll_bucket_async(interval=60):
    last_hash = None
    async with aiohttp.ClientSession() as session:
        while True:
            content = await download_from_bucket_async(session, BUCKET_NAME, FILE_NAME)
            if content:
                new_hash = hash(content)
                if new_hash != last_hash:
                    last_hash = new_hash
                    yield content
            await asyncio.sleep(interval)

# ----------------------
# Processamento CSV em chunks paralelo
# ----------------------
async def process_chunk(chunk):
    chunk_mapped = map_dataframe(chunk)
    financial_data = await enrich_chunk(chunk_mapped)  # async enrichment
    financial_df = pd.json_normalize(financial_data)
    chunk_enriched = pd.concat([chunk_mapped.reset_index(drop=True), financial_df], axis=1)
    return chunk_enriched

async def process_csv_stream_async(content, chunk_size=200):
    print("[PROCESSOR] Processando CSV em stream assíncrono...")
    os.makedirs(os.path.dirname(PROCESSED_PATH), exist_ok=True)

    first_chunk = True
    chunks = []
    for chunk in pd.read_csv(io.BytesIO(content), chunksize=chunk_size):
        chunks.append(chunk)

    tasks = [process_chunk(chunk) for chunk in chunks]
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

# ----------------------
# Envio assíncrono para XML Service
# ----------------------
async def send_to_xml_service_async(csv_path):
    id_req = str(uuid.uuid4())
    async with aiohttp.ClientSession() as session:
        with open(csv_path, "rb") as f:
            data = aiohttp.FormData()
            data.add_field("file", f, filename="acoes.csv", content_type="text/csv")
            data.add_field("ID_Requisicao", id_req)
            data.add_field("MAPPER_VERSION", MAPPER_VERSION)
            data.add_field("WEBHOOK_URL", JAVA_WEBHOOK_URL)

            async with session.post(WEBHOOK_XML_URL, data=data, timeout=60) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"[ERROR] XML Service: {text}")

    PENDING_REQUESTS[id_req] = {"csv": csv_path}
    return id_req

# ----------------------
# Webhook Flask em thread separada
# ----------------------
def start_flask_webhook():
    threading.Thread(
        target=lambda: webhook_app.run(host="0.0.0.0", port=PROCESSOR_WEBHOOK_PORT),
        daemon=True
    ).start()

# ----------------------
# Loop principal assíncrono
# ----------------------
async def main_loop_async():
    start_flask_webhook()
    print("[PROCESSOR] Monitorização do bucket iniciada...")

    async for content in poll_bucket_async(interval=60):
        print("[PROCESSOR] Novo CSV detectado. Processando...")
        df_processed = await process_csv_stream_async(content)

        print("[PROCESSOR] Enviando dados para XML Service...")
        try:
            id_req = await send_to_xml_service_async(df_processed)
            print(f"[PROCESSOR] Requisição enviada: {id_req}")
        except Exception as e:
            print(f"[PROCESSOR] Erro ao enviar para XML Service: {e}")

        print("[PROCESSOR] Aguardando próxima atualização do bucket...")

# ----------------------
# Entry point
# ----------------------
if __name__ == "__main__":
    asyncio.run(main_loop_async())
