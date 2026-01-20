import asyncio
from webhook import start_flask_webhook
from bucket import poll_bucket_async
from processing import process_csv_stream_async
from xml_client import send_to_xml_service_async
from config import PROCESSOR_WEBHOOK_PORT

async def main_loop_async():
    start_flask_webhook(PROCESSOR_WEBHOOK_PORT)
    print("[PROCESSOR] Monitorização do bucket iniciada...")

    async for content in poll_bucket_async(interval=60):
        print("[PROCESSOR] Novo CSV detectado. Processando...")
        csv_path = await process_csv_stream_async(content)

        print("[PROCESSOR] Enviando dados para XML Service...")
        try:
            id_req = await send_to_xml_service_async(csv_path)
            print(f"[PROCESSOR] Requisição enviada: {id_req}")
        except Exception as e:
            print(f"[PROCESSOR] Erro ao enviar para XML Service: {e}")

if __name__ == "__main__":
    asyncio.run(main_loop_async())
