from fastapi import FastAPI, UploadFile, Form, HTTPException
from app.xml_handler import csv_to_xml_string, validate_xml
from app.db_client import insert_xml_document
from app.webhook_client import send_webhook
import asyncio
import os

app = FastAPI()

@app.post("/process_csv")
async def process_csv(
    file: UploadFile,
    ID_Requisicao: str = Form(...),
    MAPPER_VERSION: str = Form(...),
    WEBHOOK_URL: str = Form(...)
):
    content = await file.read()
    tmp_path = f"/tmp/{file.filename}"
    with open(tmp_path, "wb") as f:
        f.write(content)

    # Gerar XML
    xml_string = csv_to_xml_string(tmp_path, MAPPER_VERSION, ID_Requisicao)

    # Validar XML
    if not validate_xml(xml_string):
        await send_webhook(WEBHOOK_URL, ID_Requisicao, "ERRO_VALIDACAO", None)
        raise HTTPException(status_code=400, detail="XML inválido")

    # Persistir no DB
    try:
        doc_id = insert_xml_document(
            ID_Requisicao,
            xml_string,
            mapper_version=MAPPER_VERSION,
            status="OK"
        )
    except Exception as e:
        await send_webhook(WEBHOOK_URL, ID_Requisicao, "ERRO_PERSISTENCIA", None)
        raise HTTPException(status_code=500, detail="Erro ao salvar XML")

    # Enviar webhook de confirmação
    await send_webhook(WEBHOOK_URL, ID_Requisicao, "OK", doc_id)
    return {"message": "XML processado com sucesso", "doc_id": doc_id}

@app.get("/query_xml")
def query_xml(xpath: str, latest: bool = False, doc_id: int | None = None):
    """
    Consulta XML persistido no DB via XPath
    """
    from app.db_client import query_xml
    results = query_xml(xpath, latest=latest, doc_id=doc_id)
    return {"results": results}
