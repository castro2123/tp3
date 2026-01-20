import aiohttp
import asyncio

async def send_webhook(webhook_url, request_id, status, doc_id):
    async with aiohttp.ClientSession() as session:
        payload = {
            "ID_Requisicao": request_id,
            "Status": status,
            "Doc_ID": doc_id
        }
        try:
            async with session.post(webhook_url, json=payload) as resp:
                if resp.status != 200:
                    print(f"[WEBHOOK] Falha ao enviar {request_id}: {resp.status}")
        except Exception as e:
            print(f"[WEBHOOK] Erro ao enviar {request_id}: {e}")
