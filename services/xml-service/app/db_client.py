import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS")

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def insert_xml_document(request_id, xml_data, mapper_version, status="OK"):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO xml_documents (request_id, xml_document, mapper_version, status)
        VALUES (%s, %s::xml, %s, %s)
        RETURNING id;
    """, (request_id, xml_data, mapper_version, status))
    doc_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return doc_id

def query_xml(xpath_query, latest=False, doc_id=None):
    """
    Consulta XML usando XPath diretamente no PostgreSQL.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if doc_id is not None:
        cur.execute(
            """
            SELECT
                id,
                request_id,
                xpath(%s, xml_document)::text[] AS matches
            FROM xml_documents
            WHERE id = %s;
            """,
            (xpath_query, doc_id),
        )
    elif latest:
        cur.execute(
            """
            SELECT
                id,
                request_id,
                xpath(%s, xml_document)::text[] AS matches
            FROM xml_documents
            WHERE id = (SELECT max(id) FROM xml_documents);
            """,
            (xpath_query,),
        )
    else:
        cur.execute(
            """
            SELECT
                id,
                request_id,
                xpath(%s, xml_document)::text[] AS matches
            FROM xml_documents;
            """,
            (xpath_query,),
        )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
