import uuid
import ssl
import aiohttp
try:
    import certifi
except ImportError:
    certifi = None
from config import WEBHOOK_XML_URL, JAVA_WEBHOOK_URL, PENDING_REQUESTS, FILE_NAME
from rpc_client import fetch_mapper_version

async def send_to_xml_service_async(csv_path):
    id_req = str(uuid.uuid4())
    if not WEBHOOK_XML_URL:
        raise RuntimeError("WEBHOOK_XML_URL nao definido no .env")
    if not JAVA_WEBHOOK_URL:
        raise RuntimeError("JAVA_WEBHOOK_URL nao definido no .env")
    mapper_version = fetch_mapper_version()
    ssl_context = ssl.create_default_context(cafile=certifi.where()) if certifi else None
    async with aiohttp.ClientSession() as session:
        with open(csv_path, "rb") as f:
            data = aiohttp.FormData()
            data.add_field("file", f, filename="acoes.csv", content_type="text/csv")
            data.add_field("ID_Requisicao", id_req)
            data.add_field("MAPPER_VERSION", mapper_version)
            data.add_field("WEBHOOK_URL", JAVA_WEBHOOK_URL)

            async with session.post(
                WEBHOOK_XML_URL,
                data=data,
                timeout=60,
                ssl=ssl_context
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"[ERROR] XML Service: {text}")

    PENDING_REQUESTS[id_req] = {"csv": csv_path, "bucket": FILE_NAME}
    return id_req
