import pandas as pd
from lxml import etree
import io

def csv_to_xml_string(csv_path):
    """
    Converte CSV em XML
    """
    df = pd.read_csv(csv_path)
    root = etree.Element("root")

    for _, row in df.iterrows():
        item = etree.SubElement(root, "item")
        for col, val in row.items():
            child = etree.SubElement(item, col)
            child.text = str(val) if not pd.isna(val) else ""

    xml_bytes = etree.tostring(root, pretty_print=True, encoding="utf-8", xml_declaration=True)
    return xml_bytes.decode("utf-8")

def validate_xml(xml_string):
    """
    Valida XML basico (well-formed)
    """
    try:
        etree.fromstring(xml_string.encode("utf-8"))
        return True
    except etree.XMLSyntaxError:
        return False
