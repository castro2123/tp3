from flask import Flask, request, jsonify
import os

app = Flask(__name__)

@app.route("/webhook/status", methods=["POST"])
def status():
    data = request.json
    status = data.get("Status")
    print("[PROCESSOR] Callback recebido:", data)

    if status == "OK":
        csv = data.get("CSV_PATH")
        if csv and os.path.exists(csv):
            os.remove(csv)

    return jsonify({"ok": True})
