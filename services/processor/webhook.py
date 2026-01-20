import asyncio
import os
from flask import Flask, request
from config import PENDING_REQUESTS
from bucket import delete_from_bucket_async

app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook_handler():
    data = request.json
    id_req = data.get("ID_Requisicao")
    status = data.get("Status")
    doc_id = data.get("Doc_ID")
    print(f"[WEBHOOK] Requisição {id_req} status={status}, doc_id={doc_id}")

    if id_req in PENDING_REQUESTS:
        info = PENDING_REQUESTS.pop(id_req)
        if status == "OK":
            csv_path = info.get("csv")
            if csv_path and os.path.exists(csv_path):
                os.remove(csv_path)
                print(f"[WEBHOOK] CSV removido: {csv_path}")
            bucket_file = info.get("bucket")
            if bucket_file:
                asyncio.run(delete_from_bucket_async(file_name=bucket_file))
    return {"message": "OK"}, 200

def start_flask_webhook(port=5000):
    import threading
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=port), daemon=True).start()
