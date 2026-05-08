from __future__ import annotations

from collections import defaultdict
from xml.etree import ElementTree as ET


SOAPENV = "http://schemas.xmlsoap.org/soap/envelope/"
SERVICE_NS = "http://uniware.unicommerce.com/services/"
WSSE_NS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"

NSMAP = {
    "soapenv": SOAPENV,
    "ser": SERVICE_NS,
    "wsse": WSSE_NS,
}


def qname(namespace: str, tag: str) -> str:
    return f"{{{namespace}}}{tag}"


def add_text_element(parent: ET.Element, namespace: str, tag: str, value: object | None) -> ET.Element | None:
    if value is None:
        return None
    child = ET.SubElement(parent, qname(namespace, tag))
    child.text = str(value)
    return child


def element_to_string(element: ET.Element) -> str:
    return ET.tostring(element, encoding="unicode")


def strip_namespace(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def xml_to_dict(element: ET.Element) -> dict | str | None:
    children = list(element)
    if not children:
        return (element.text or "").strip() or None

    grouped: dict[str, list] = defaultdict(list)
    for child in children:
        grouped[strip_namespace(child.tag)].append(xml_to_dict(child))

    result: dict[str, object] = {}
    for key, values in grouped.items():
        result[key] = values[0] if len(values) == 1 else values

    for attr_key, attr_value in element.attrib.items():
        result[f"@{strip_namespace(attr_key)}"] = attr_value
    return result
