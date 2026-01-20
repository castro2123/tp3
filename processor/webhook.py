from flask import Flask, request
from config import PENDING_REQUESTS

app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook_handler():
    data = request.json
    id_req = data.get("ID_Requisicao")
    status = data.get("Status")
    doc_id = data.get("Doc_ID")
    print(f"[WEBHOOK] Requisição {id_req} status={status}, doc_id={doc_id}")

    if id_req in PENDING_REQUESTS:
        del PENDING_REQUESTS[id_req]
    return {"message": "OK"}, 200

def start_flask_webhook(port=5000):
    import threading
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=port), daemon=True).start()
