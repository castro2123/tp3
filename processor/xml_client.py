import aiohttp
import uuid
from config import WEBHOOK_XML_URL, JAVA_WEBHOOK_URL, MAPPER_VERSION, PENDING_REQUESTS

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
