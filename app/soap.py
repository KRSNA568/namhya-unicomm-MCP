from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from xml.etree import ElementTree as ET

import httpx

from app.config import Settings
from app.xml_utils import NSMAP, SERVICE_NS, SOAPENV, WSSE_NS, add_text_element, element_to_string, qname, xml_to_dict

# Register prefixes so ET serialises soapenv:/ser:/wsse: instead of ns0:/ns1:/ns2:.
for _prefix, _uri in NSMAP.items():
    ET.register_namespace(_prefix, _uri)

logger = logging.getLogger(__name__)


class SoapFaultError(RuntimeError):
    pass


@dataclass(slots=True)
class SoapResponse:
    operation: str
    request_xml: str
    response_xml: str
    payload: dict | str | None


class UnicommerceSoapClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._client = httpx.AsyncClient(timeout=settings.unicommerce_timeout_seconds)

    async def close(self) -> None:
        await self._client.aclose()

    def build_envelope(self, operation: str, body_element: ET.Element) -> str:
        envelope = ET.Element(qname(SOAPENV, "Envelope"))
        # Do NOT manually set xmlns: attributes — register_namespace() above
        # already ensures ET serialises the correct prefix names.

        header = ET.SubElement(envelope, qname(SOAPENV, "Header"))
        security = ET.SubElement(header, qname(WSSE_NS, "Security"))
        security.set(qname(SOAPENV, "mustUnderstand"), "1")
        token = ET.SubElement(security, qname(WSSE_NS, "UsernameToken"))
        add_text_element(token, WSSE_NS, "Username", self.settings.unicommerce_username)
        pwd = add_text_element(token, WSSE_NS, "Password", self.settings.unicommerce_password)
        if pwd is not None:
            pwd.set(
                "Type",
                "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText",
            )

        body = ET.SubElement(envelope, qname(SOAPENV, "Body"))
        body.append(body_element)
        return element_to_string(envelope)

    async def call(self, operation: str, body_element: ET.Element) -> SoapResponse:
        request_xml = self.build_envelope(operation, body_element)
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": "",
        }

        url = self.settings.unicommerce_base_url
        if self.settings.unicommerce_facility:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}facility={self.settings.unicommerce_facility}"

        last_error: Exception | None = None
        for attempt in range(1, self.settings.unicommerce_max_retries + 1):
            try:
                response = await self._client.post(url, content=request_xml, headers=headers)
                response.raise_for_status()
                payload = self._parse_response(response.text)
                return SoapResponse(operation=operation, request_xml=request_xml, response_xml=response.text, payload=payload)
            except (httpx.HTTPError, SoapFaultError, ET.ParseError) as exc:
                last_error = exc
                logger.warning("SOAP call failed", extra={"operation": operation, "attempt": attempt, "error": str(exc)})
                if attempt == self.settings.unicommerce_max_retries:
                    break
                await asyncio.sleep(self.settings.unicommerce_retry_backoff_seconds * attempt)

        raise RuntimeError(f"SOAP call failed for {operation}") from last_error

    def _parse_response(self, xml_text: str) -> dict | str | None:
        root = ET.fromstring(xml_text)
        fault = root.find(".//soapenv:Fault", NSMAP)
        if fault is not None:
            raise SoapFaultError(element_to_string(fault))

        body = root.find("soapenv:Body", NSMAP)
        if body is None or len(body) == 0:
            return None
        response_node = body[0]
        return xml_to_dict(response_node)
