from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from dateutil import parser as date_parser


def ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def parse_decimal(value: Any) -> Decimal | None:
    if value in (None, "", "None"):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def parse_int(value: Any) -> int | None:
    if value in (None, "", "None"):
        return None
    try:
        return int(value)
    except Exception:
        return None


def parse_bool(value: Any) -> bool | None:
    if value in (None, "", "None"):
        return None
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"true", "1", "yes"}


def parse_datetime(value: Any) -> datetime | None:
    if value in (None, "", "None"):
        return None
    try:
        dt = date_parser.isoparse(str(value))
    except Exception:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _pick_shipping_address(sale_order: dict) -> dict[str, Any]:
    addresses = ensure_list(sale_order.get("Addresses", {}).get("Address") if isinstance(sale_order.get("Addresses"), dict) else None)
    if not addresses:
        return {}
    shipping_ref = sale_order.get("ShippingAddress") or {}
    shipping_id = shipping_ref.get("id") or shipping_ref.get("@id") or shipping_ref.get("Id")
    for address in addresses:
        address_id = address.get("Id") or address.get("@id") or address.get("id")
        if shipping_id and address_id == shipping_id:
            return address
    return addresses[0]


def normalize_order(payload: dict) -> dict[str, Any]:
    sale_order = payload.get("SaleOrder") or {}
    address = _pick_shipping_address(sale_order)
    items_raw = ensure_list((sale_order.get("SaleOrderItems") or {}).get("SaleOrderItem"))

    items: list[dict[str, Any]] = []
    total_revenue = Decimal("0")
    facilities: list[dict[str, Any]] = []
    returns: list[dict[str, Any]] = []

    for item in items_raw:
        quantity = parse_int(item.get("Quantity")) or 1
        total_price = parse_decimal(item.get("TotalPrice"))
        selling_price = parse_decimal(item.get("SellingPrice"))
        discount = parse_decimal(item.get("Discount"))
        shipping_charges = parse_decimal(item.get("ShippingCharges"))
        item_revenue = total_price or (selling_price * quantity if selling_price is not None else Decimal("0"))
        total_revenue += item_revenue

        facility_code = item.get("FacilityCode")
        facility_name = item.get("FacilityName")
        if facility_code:
            facilities.append({"code": facility_code, "name": facility_name})

        normalized_item = {
            "order_item_code": item.get("Code"),
            "sku_code": item.get("ItemSKU"),
            "channel_product_id": item.get("ChannelProductId"),
            "status": item.get("StatusCode") or item.get("Status"),
            "quantity": quantity,
            "selling_price": selling_price,
            "total_price": total_price,
            "discount": discount,
            "shipping_charges": shipping_charges,
            "prepaid_amount": parse_decimal(item.get("PrepaidAmount")),
            "facility_code": facility_code,
            "facility_name": facility_name,
            "cancellable": parse_bool(item.get("Cancellable")),
            "cancellation_reason": item.get("CancellationReason"),
            "reverse_pickable": parse_bool(item.get("ReversePickable")),
            "created_on": parse_datetime(item.get("CreatedOn")),
            "updated_on": parse_datetime(item.get("UpdatedOn")),
            "raw_payload": item,
        }
        items.append(normalized_item)

        status = (normalized_item["status"] or "").upper()
        if "CANCEL" in status or normalized_item["cancellation_reason"]:
            returns.append(
                {
                    "record_key": f"{normalized_item['order_item_code']}:cancellation",
                    "order_item_code": normalized_item["order_item_code"],
                    "sku_code": normalized_item["sku_code"],
                    "status": normalized_item["status"],
                    "record_type": "cancellation",
                    "reason": normalized_item["cancellation_reason"],
                    "amount": item_revenue,
                    "warehouse_code": facility_code,
                    "event_time": normalized_item["updated_on"] or parse_datetime(sale_order.get("UpdatedOn")),
                    "raw_payload": item,
                }
            )
        elif "RETURN" in status:
            returns.append(
                {
                    "record_key": f"{normalized_item['order_item_code']}:return",
                    "order_item_code": normalized_item["order_item_code"],
                    "sku_code": normalized_item["sku_code"],
                    "status": normalized_item["status"],
                    "record_type": "return",
                    "reason": item.get("Reason"),
                    "amount": item_revenue,
                    "warehouse_code": facility_code,
                    "event_time": normalized_item["updated_on"] or parse_datetime(sale_order.get("UpdatedOn")),
                    "raw_payload": item,
                }
            )

    return {
        "order": {
            "order_code": sale_order.get("Code"),
            "display_order_code": sale_order.get("DisplayOrderCode"),
            "channel_code": sale_order.get("Channel"),
            "status": sale_order.get("Status"),
            "cash_on_delivery": parse_bool(sale_order.get("CashOnDelivery")),
            "customer_email": sale_order.get("NotificationEmail"),
            "customer_mobile": sale_order.get("NotificationMobile"),
            "customer_name": address.get("Name"),
            "shipping_city": address.get("City"),
            "shipping_state": address.get("State"),
            "shipping_country": address.get("Country"),
            "shipping_postcode": address.get("Pincode") or address.get("PostalCode"),
            "payment_mode": sale_order.get("PaymentMode"),
            "currency_code": sale_order.get("CurrencyCode"),
            "created_on": parse_datetime(sale_order.get("CreatedOn")),
            "updated_on": parse_datetime(sale_order.get("UpdatedOn")),
            "display_order_datetime": parse_datetime(sale_order.get("DisplayOrderDateTime")),
            "fulfillment_tat": parse_datetime(sale_order.get("FulfillmentTat")),
            "amount_paid": parse_decimal(sale_order.get("AmountPaid")),
            "total_revenue": total_revenue,
            "raw_payload": sale_order,
        },
        "items": items,
        "channel": {"code": sale_order.get("Channel"), "name": sale_order.get("Channel")},
        "warehouses": facilities,
        "returns": returns,
    }


def normalize_inventory_search_response(payload: dict) -> dict[str, Any]:
    item_types = ensure_list((payload.get("ItemTypes") or {}).get("ItemType"))
    skus: list[dict[str, Any]] = []
    inventory_snapshots: list[dict[str, Any]] = []
    warehouses: list[dict[str, Any]] = []

    for item_type in item_types:
        sku_code = item_type.get("SKUCode")
        skus.append(
            {
                "sku_code": sku_code,
                "name": item_type.get("Name"),
                "category_name": item_type.get("CategoryName"),
                "category_code": item_type.get("CategoryCode"),
                "brand": item_type.get("Brand"),
                "size": item_type.get("Size"),
                "color": item_type.get("Color"),
                "hsn_code": item_type.get("HSNCode"),
                "price": parse_decimal(item_type.get("Price")),
                "enabled": True,
                "raw_payload": item_type,
            }
        )
        snapshots = ensure_list((item_type.get("InventorySnapshots") or {}).get("InventorySnapshot"))
        for snapshot in snapshots:
            facility_code = snapshot.get("Facility")
            warehouses.append({"code": facility_code, "name": facility_code})
            inventory_snapshots.append(
                {
                    "sku_code": sku_code,
                    "product_name": item_type.get("Name"),
                    "warehouse_code": facility_code,
                    "warehouse_name": facility_code,
                    "inventory": parse_int(snapshot.get("Inventory")) or 0,
                    "open_sale": parse_int(snapshot.get("OpenSale")) or 0,
                    "open_purchase": parse_int(snapshot.get("OpenPurchase")) or 0,
                    "putaway_pending": parse_int(snapshot.get("PutawayPending")) or 0,
                    "inventory_blocked": parse_int(snapshot.get("InventoryBlocked")) or 0,
                    "pending_inventory_assessment": parse_int(snapshot.get("PendingInventoryAssessment")),
                    "pending_stock_transfer": parse_int(snapshot.get("PendingStockTransfer")),
                    "vendor_inventory": parse_int(snapshot.get("VendorInventory")),
                    "raw_payload": snapshot,
                }
            )

    return {"skus": skus, "inventory_snapshots": inventory_snapshots, "warehouses": warehouses}


def normalize_shipment_search_response(payload: dict, delay_threshold_hours: int) -> list[dict[str, Any]]:
    shipments_raw = ensure_list((payload.get("ShippingPackages") or {}).get("ShippingPackage"))
    shipments: list[dict[str, Any]] = []
    for shipment in shipments_raw:
        created_on = parse_datetime(shipment.get("CreatedOn"))
        updated_on = parse_datetime(shipment.get("UpdatedOn"))
        status = shipment.get("Status")
        delivered_at = updated_on if status and status.upper() == "DELIVERED" else None
        delay_hours = None
        if created_on and updated_on:
            transit_hours = int((updated_on - created_on).total_seconds() // 3600)
            delay_hours = max(0, transit_hours - delay_threshold_hours)

        shipments.append(
            {
                "shipment_code": shipment.get("Code"),
                "sale_order_code": shipment.get("SaleOrderCode"),
                "status": status,
                "shipping_provider": shipment.get("ShippingProvider"),
                "shipping_method": shipment.get("ShippingMethod"),
                "tracking_number": shipment.get("TrackingNumber"),
                "courier_status": shipment.get("CourierStatus"),
                "tracking_status": shipment.get("CourierStatus"),
                "invoice_code": shipment.get("InvoiceCode"),
                "invoice_display_code": shipment.get("InvoiceDisplayCode"),
                "facility_code": None,
                "facility_name": None,
                "created_on": created_on,
                "updated_on": updated_on,
                "promised_by": None,
                "delivered_at": delivered_at,
                "delivery_delay_hours": delay_hours,
                "raw_payload": shipment,
            }
        )
    return shipments


def normalize_purchase_orders_response(payload: dict) -> list[str]:
    pos = payload.get("PurchaseOrders") or {}
    raw = pos.get("PurchaseOrder") if isinstance(pos, dict) else []
    if not isinstance(raw, list):
        raw = [raw] if raw else []
    codes = []
    for p in raw:
        if isinstance(p, dict):
            code = p.get("Code")
            if code:
                codes.append(str(code))
        elif p:
            codes.append(str(p))
    return codes


def normalize_purchase_order_detail(payload: dict) -> dict[str, Any]:
    items_raw = ensure_list((payload.get("PurchaseOrderItems") or {}).get("PurchaseOrderItem"))
    items = []
    for item in items_raw:
        items.append({
            "sku_code": item.get("ItemSKU"),
            "quantity": parse_int(item.get("Quantity")),
            "unit_price": parse_decimal(item.get("UnitPrice")),
            "mrp": parse_decimal(item.get("MaxRetailPrice")),
            "discount": parse_decimal(item.get("Discount")),
            "tax_type": item.get("TaxTypeCode"),
        })
    return {
        "po_code": payload.get("Code"),
        "status": payload.get("StatusCode"),
        "vendor_code": payload.get("VendorCode"),
        "vendor_name": payload.get("VendorName"),
        "type": payload.get("Type"),
        "created": payload.get("Created"),
        "expiry_date": payload.get("ExpiryDate"),
        "delivery_date": payload.get("DeliveryDate"),
        "inflow_receipts_count": parse_int(payload.get("InflowReceiptsCount")),
        "items": items,
    }


def normalize_inflow_receipts_response(payload: dict) -> list[str]:
    receipts = payload.get("InflowReceipts") or {}
    raw = receipts.get("InflowReceipt") if isinstance(receipts, dict) else []
    if not isinstance(raw, list):
        raw = [raw] if raw else []
    codes = []
    for r in raw:
        if isinstance(r, dict):
            code = r.get("Code")
            if code:
                codes.append(str(code))
        elif r:
            codes.append(str(r))
    return codes


def normalize_inflow_receipt_detail(payload: dict) -> list[dict[str, Any]]:
    receipts_raw = ensure_list(payload.get("InflowReceipt"))
    result = []
    for receipt in receipts_raw:
        items_raw = ensure_list((receipt.get("InflowReceiptItems") or {}).get("InflowReceiptItem"))
        po = receipt.get("PurchaseOrderDTO") or {}
        items = []
        for item in items_raw:
            items.append({
                "sku_code": item.get("ItemSKU"),
                "product_name": item.get("ItemTypeName"),
                "vendor_sku": item.get("VendorSkuCode"),
                "quantity_received": parse_int(item.get("Quantity")),
                "quantity_rejected": parse_int(item.get("RejectedQuantity")),
                "quantity_pending": parse_int(item.get("PendingQuantity")),
                "unit_price": parse_decimal(item.get("UnitPrice")),
                "batch_code": item.get("BatchCode"),
                "expiry_date": item.get("Expiry"),
                "manufacturing_date": item.get("ManufacturingDate"),
                "status": item.get("Status"),
            })
        result.append({
            "receipt_code": receipt.get("Code"),
            "status": receipt.get("StatusCode"),
            "created": receipt.get("Created"),
            "vendor_invoice_number": receipt.get("VendorInvoiceNumber"),
            "vendor_invoice_date": receipt.get("VendorInvoiceDate"),
            "total_quantity": parse_int(receipt.get("TotalQuantity")),
            "total_received_amount": parse_decimal(receipt.get("TotalReceivedAmount")),
            "po_code": po.get("Code"),
            "vendor_code": po.get("VendorCode"),
            "vendor_name": po.get("VendorName"),
            "items": items,
        })
    return result


def normalize_back_order_items(payload: dict) -> list[dict[str, Any]]:
    items_raw = ensure_list((payload.get("BackOrderItems") or {}).get("BackOrderItem"))
    return [
        {
            "sku_code": item.get("SkuCode"),
            "product_name": item.get("Name"),
            "quantity": parse_int(item.get("Quantity")),
            "brand": item.get("Brand"),
            "size": item.get("Size"),
            "color": item.get("Color"),
        }
        for item in items_raw
    ]


def normalize_vendors(payload: dict) -> list[dict[str, Any]]:
    vendor_list = payload.get("VendorList") or {}
    raw = vendor_list.get("Vendor") if isinstance(vendor_list, dict) else []
    if not isinstance(raw, list):
        raw = [raw] if raw else []
    return [
        {
            "vendor_code": v.get("VendorCode"),
            "name": v.get("Name"),
            "gst_number": v.get("GSTNumber"),
            "pan": v.get("PAN"),
            "email": v.get("Email"),
            "phone": v.get("Phone"),
            "city": v.get("City"),
            "enabled": parse_bool(v.get("Enabled")),
        }
        for v in raw
    ]


def normalize_vendor_item_types(payload: dict) -> dict[str, Any]:
    items_raw = ensure_list((payload.get("VendorItemTypes") or {}).get("VendorItemType"))
    return {
        "total_records": parse_int(payload.get("TotalRecords")),
        "page_number": parse_int(payload.get("PageNumber")),
        "page_size": parse_int(payload.get("PageSize")),
        "items": [
            {
                "vendor_code": item.get("VendorCode"),
                "sku_code": item.get("ItemSKU"),
                "product_name": item.get("ItemTypeName"),
                "vendor_sku": item.get("SellerSkuCode"),
                "unit_price": parse_decimal(item.get("UnitPrice")),
                "mrp": parse_decimal(item.get("MaxRetailPrice")),
                "enabled": parse_bool(item.get("Enabled")),
                "brand": item.get("Brand"),
                "size": item.get("Size"),
            }
            for item in items_raw
        ],
    }


def normalize_channel(payload: dict) -> dict[str, Any] | None:
    detail = payload.get("ChannelDetailDTO")
    if not detail:
        return None
    return {
        "code": detail.get("Code"),
        "name": detail.get("Name") or detail.get("ShortName") or detail.get("Code"),
        "enabled": parse_bool(detail.get("Enabled")),
        "raw_payload": detail,
    }
