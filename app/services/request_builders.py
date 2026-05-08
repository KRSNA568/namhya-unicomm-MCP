from __future__ import annotations

from datetime import datetime
from xml.etree import ElementTree as ET

from app.xml_utils import SERVICE_NS, add_text_element, qname


def _root(name: str) -> ET.Element:
    return ET.Element(qname(SERVICE_NS, name))


def _search_options(parent: ET.Element, start: int, length: int) -> None:
    search_options = ET.SubElement(parent, qname(SERVICE_NS, "SearchOptions"))
    add_text_element(search_options, SERVICE_NS, "DisplayStart", start)
    add_text_element(search_options, SERVICE_NS, "DisplayLength", length)


def _date(parent: ET.Element, tag: str, value: datetime | None) -> None:
    if value is not None:
        add_text_element(parent, SERVICE_NS, tag, value.isoformat())


def build_search_sale_order_request(
    *,
    start: int,
    length: int,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    updated_since_minutes: int | None = None,
    return_statuses: list[str] | None = None,
) -> ET.Element:
    root = _root("SearchSaleOrderRequest")
    _search_options(root, start, length)
    _date(root, "FromDate", from_date)
    _date(root, "ToDate", to_date)
    if updated_since_minutes is not None:
        add_text_element(root, SERVICE_NS, "UpdatedSinceInMinutes", updated_since_minutes)
    if return_statuses:
        statuses = ET.SubElement(root, qname(SERVICE_NS, "ReturnStatuses"))
        for status in return_statuses:
            add_text_element(statuses, SERVICE_NS, "ReturnStatus", status)
    return root


def build_get_sale_order_request(order_code: str) -> ET.Element:
    root = _root("GetSaleOrderRequest")
    sale_order = ET.SubElement(root, qname(SERVICE_NS, "SaleOrder"))
    add_text_element(sale_order, SERVICE_NS, "Code", order_code)
    add_text_element(root, SERVICE_NS, "IsPaymentDetailRequired", "true")
    return root


def build_search_shipping_package_request(
    *,
    start: int,
    length: int,
    created_from: datetime | None = None,
    created_to: datetime | None = None,
    updated_since_minutes: int | None = None,
) -> ET.Element:
    root = _root("SearchShippingPackageRequest")
    if created_from or created_to:
        create_time = ET.SubElement(root, qname(SERVICE_NS, "CreateTime"))
        _date(create_time, "Start", created_from)
        _date(create_time, "End", created_to)
    if updated_since_minutes is not None:
        add_text_element(root, SERVICE_NS, "UpdatedSinceInMinutes", updated_since_minutes)
    _search_options(root, start, length)
    return root


def build_search_item_types_request(*, start: int, length: int, include_inventory_snapshot: bool = True) -> ET.Element:
    root = _root("SearchItemTypesRequest")
    add_text_element(root, SERVICE_NS, "GetInventorySnapshot", str(include_inventory_snapshot).lower())
    _search_options(root, start, length)
    return root


def build_get_channel_details_request(channel_code: str) -> ET.Element:
    root = _root("GetChannelDetailsRequest")
    add_text_element(root, SERVICE_NS, "ChannelCode", channel_code)
    return root


def build_get_purchase_orders_request(
    *,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    vendor_name: str | None = None,
) -> ET.Element:
    root = _root("GetPurchaseOrdersRequest")
    if vendor_name:
        add_text_element(root, SERVICE_NS, "VendorName", vendor_name)
    if from_date or to_date:
        date_range = ET.SubElement(root, qname(SERVICE_NS, "CreatedDateRange"))
        if from_date:
            add_text_element(date_range, SERVICE_NS, "Start", from_date.isoformat())
        if to_date:
            add_text_element(date_range, SERVICE_NS, "End", to_date.isoformat())
    return root


def build_get_purchase_order_detail_request(po_code: str) -> ET.Element:
    root = _root("GetPurchaseOrderDetailRequest")
    add_text_element(root, SERVICE_NS, "PurchaseOrderCode", po_code)
    return root


def build_get_inflow_receipts_request(
    *,
    po_code: str | None = None,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
) -> ET.Element:
    root = _root("GetInflowReceiptsRequest")
    if po_code:
        add_text_element(root, SERVICE_NS, "PurchaseOrderCode", po_code)
    if from_date or to_date:
        date_range = ET.SubElement(root, qname(SERVICE_NS, "CreatedDateRange"))
        if from_date:
            add_text_element(date_range, SERVICE_NS, "Start", from_date.isoformat())
        if to_date:
            add_text_element(date_range, SERVICE_NS, "End", to_date.isoformat())
    return root


def build_get_inflow_receipt_detail_request(receipt_code: str) -> ET.Element:
    root = _root("GetInflowReceiptDetailRequest")
    add_text_element(root, SERVICE_NS, "InflowReceiptCode", receipt_code)
    return root


def build_get_back_order_items_request(*, start: int = 0) -> ET.Element:
    root = _root("GetBackOrderItemsRequest")
    search_options = ET.SubElement(root, qname(SERVICE_NS, "SearchOptions"))
    add_text_element(search_options, SERVICE_NS, "DisplayStart", start)
    return root


def build_get_vendors_request(*, from_date: datetime, to_date: datetime) -> ET.Element:
    root = _root("GetVendorsRequest")
    date_range = ET.SubElement(root, qname(SERVICE_NS, "DateRange"))
    add_text_element(date_range, SERVICE_NS, "Start", from_date.isoformat())
    add_text_element(date_range, SERVICE_NS, "End", to_date.isoformat())
    return root


def build_get_vendor_item_types_request(*, page_number: int = 1, page_size: int = 50) -> ET.Element:
    root = _root("GetVendorItemTypesRequest")
    add_text_element(root, SERVICE_NS, "PageNumber", page_number)
    add_text_element(root, SERVICE_NS, "PageSize", page_size)
    return root
