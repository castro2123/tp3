import os
from datetime import datetime, timezone
import pandas as pd
from lxml import etree

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.xsd")
_SCHEMA_CACHE = None

def csv_to_xml_string(csv_path, mapper_version, request_id):
    """
    Converte CSV em um XML do dominio (desacoplado da origem).
    """
    df = pd.read_csv(csv_path)

    root = etree.Element("RelatorioMercado")
    root.set("dataGeracao", datetime.now(timezone.utc).isoformat(timespec="seconds"))
    root.set("versao", "1.0")
    root.set("mapperVersao", str(mapper_version))
    root.set("requestId", str(request_id))

    config = etree.SubElement(root, "Configuracao")
    config.set("validadoPor", "XMLService")
    config.set("requisitante", str(request_id))

    ativos = etree.SubElement(root, "Ativos")

    for idx, row in df.iterrows():
        ativo = etree.SubElement(ativos, "Ativo")
        ativo.set("idInterno", f"CSV_{idx + 1}")
        _set_attr_if(ativo, "ticker", _safe_text(row.get("Ticker")))
        _set_attr_if(ativo, "mercado", _safe_text(row.get("Mercado")))

        empresa = etree.SubElement(ativo, "Empresa")
        _set_text(empresa, "Nome", _safe_text(row.get("Nome")))
        _set_text(empresa, "Sector", _safe_text(row.get("Sector")))
        _set_text(empresa, "Industria", _safe_text(row.get("Industry")))

        negociacao = etree.SubElement(ativo, "Negociacao")
        _set_text(negociacao, "UltimoPreco", _safe_text(row.get("Último_Preço")))
        _set_text(negociacao, "VariacaoPercentual", _safe_text(row.get("Variacao_%")))
        _set_text(negociacao, "DataHora", _safe_text(row.get("Data_Hora")))

        fundamentos = etree.SubElement(ativo, "Fundamentos")
        _set_text(fundamentos, "MarketCap", _safe_text(row.get("MarketCap")))
        _set_text(fundamentos, "PERatio", _safe_text(row.get("PERatio")))

        fonte = etree.SubElement(ativo, "Fonte")
        _set_text(fonte, "Link", _safe_text(row.get("Link")))

    xml_bytes = etree.tostring(
        root,
        pretty_print=True,
        encoding="utf-8",
        xml_declaration=True
    )
    return xml_bytes.decode("utf-8")

def validate_xml(xml_string):
    """
    Valida XML com XSD do dominio.
    """
    schema = _load_schema()
    if schema is None:
        return False

    try:
        doc = etree.fromstring(xml_string.encode("utf-8"))
        return schema.validate(doc)
    except etree.XMLSyntaxError:
        return False

def _load_schema():
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is not None:
        return _SCHEMA_CACHE
    if not os.path.exists(SCHEMA_PATH):
        print(f"[XML] Schema nao encontrado: {SCHEMA_PATH}")
        return None
    with open(SCHEMA_PATH, "rb") as f:
        schema_doc = etree.parse(f)
    _SCHEMA_CACHE = etree.XMLSchema(schema_doc)
    return _SCHEMA_CACHE

def _safe_text(value):
    if value is None or pd.isna(value):
        return ""
    return str(value)

def _set_text(parent, tag, value):
    child = etree.SubElement(parent, tag)
    child.text = value
    return child

def _set_attr_if(node, attr, value):
    if value:
        node.set(attr, value)
