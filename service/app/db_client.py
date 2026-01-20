import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def insert_xml_document(request_id, xml_data, status="OK"):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO xml_documents (request_id, xml_data, status)
        VALUES (%s, %s, %s)
        RETURNING id;
    """, (request_id, xml_data, status))
    doc_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return doc_id

def query_xml(xpath_query):
    """
    Consulta XML usando XPath e retorna lista de dicionários
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT id, request_id, xml_data
        FROM xml_documents;
    """)
    rows = cur.fetchall()
    results = []

    from lxml import etree
    for row in rows:
        try:
            xml_tree = etree.fromstring(row["xml_data"].encode("utf-8"))
            matches = xml_tree.xpath(xpath_query)
            results.append({
                "id": row["id"],
                "request_id": row["request_id"],
                "matches": matches
            })
        except Exception as e:
            continue

    cur.close()
    conn.close()
    return results
