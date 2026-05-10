from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from decimal import Decimal
from xml.etree import ElementTree as ET

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.server import TransportSecuritySettings

from app.config import get_settings
from app.services.normalizers import (
    normalize_back_order_items,
    normalize_channel,
    normalize_inflow_receipt_detail,
    normalize_inflow_receipts_response,
    normalize_inventory_search_response,
    normalize_order,
    normalize_purchase_order_detail,
    normalize_purchase_orders_response,
    normalize_shipment_search_response,
    normalize_vendor_item_types,
    normalize_vendors,
)
from app.services.request_builders import (
    build_get_back_order_items_request,
    build_get_channel_details_request,
    build_get_inflow_receipt_detail_request,
    build_get_inflow_receipts_request,
    build_get_purchase_order_detail_request,
    build_get_purchase_orders_request,
    build_get_sale_order_request,
    build_get_vendor_item_types_request,
    build_get_vendors_request,
    build_search_item_types_request,
    build_search_sale_order_request,
    build_search_shipping_package_request,
)
from app.soap import UnicommerceSoapClient

logger = logging.getLogger(__name__)

settings = get_settings()

FACILITY_NAMES: dict[str, str] = {
    "namhyafood": "NAMHYA FOODS DELHI",
    "namhya_jammu": "NAMHYA FOODS JAMMU",
}


def _facility_name(code: str | None) -> str | None:
    if code is None:
        return None
    return FACILITY_NAMES.get(code, code)

mcp = FastMCP(
    "Namhya Unicommerce MCP",
    instructions=(
        "You are a live ecommerce operations assistant for Namhya Foods, powered by real-time Unicommerce data.\n\n"
        "BUSINESS CONTEXT:\n"
        "Namhya Foods sells health food products on Amazon, Flipkart, and other channels.\n"
        "Two warehouses: NAMHYA FOODS DELHI (code: namhyafood) and NAMHYA FOODS JAMMU (code: namhya_jammu).\n"
        "Always show warehouse names, never codes. Use ₹ for INR.\n\n"
        "TOOLS — pick the right one for the question:\n"
        "OVERVIEW & DASHBOARDS:\n"
        "  get_order_summary(days) → orders + revenue by channel/status. START HERE for any sales question.\n"
        "  get_inventory_summary(warehouse?) → stock health: OOS count, low-stock, per-warehouse breakdown.\n"
        "  get_fulfillment_health(days) → pending orders + delayed shipments + backorders in ONE call.\n"
        "SALES & REVENUE:\n"
        "  get_revenue_trend(days, group_by) → day/week revenue chart. Use for trend questions.\n"
        "  get_channel_performance(days) → compare Amazon vs Flipkart etc: orders, revenue, cancellation %.\n"
        "  get_top_selling_skus(days, top_n, channel?) → top products by quantity and revenue.\n"
        "  search_orders(from_date, to_date, status, max_results) → raw order list with filters.\n"
        "  get_order(order_code) → full single-order detail: items, pricing, returns.\n"
        "LOGISTICS:\n"
        "  search_shipments(created_from, created_to, delayed_only) → shipments with delay detection.\n"
        "INVENTORY:\n"
        "  get_inventory(sku?, warehouse?, low_stock_only?) → per-SKU per-warehouse stock snapshot.\n"
        "  get_inventory_insights(days) → deep dive: days-of-stock, consumption rate, value, per-warehouse split.\n"
        "  get_reorder_suggestions(reorder_threshold_days) → SKUs running low with vendor + cost.\n"
        "  get_expiry_alerts(alert_days) → batches expiring soon (critical for food products).\n"
        "  get_back_order_items() → SKUs with orders but no stock — need URGENT restock.\n"
        "VENDORS & PROCUREMENT:\n"
        "  get_vendors(from_date?, to_date?) → registered vendors with contact info.\n"
        "  get_vendor_item_types(page, page_size) → which vendor supplies which SKU + pricing.\n"
        "  get_vendor_insights(months) → fill rate, dependency risk, order value per vendor.\n"
        "  get_purchase_orders(from_date?, to_date?, vendor_name?) → PO code list.\n"
        "  get_purchase_order_detail(po_code) → full PO: items, quantities, prices.\n"
        "  get_inflow_receipts(from_date?, to_date?, po_code?) → GRN code list.\n"
        "  get_inflow_receipt_detail(receipt_code) → received items, batch/expiry data.\n"
        "  analyze_procurement_needs(target_month, target_year) → suggested order quantities for next month.\n"
        "CHANNELS:\n"
        "  list_channels(days) → all active channels with order counts. USE FIRST before get_channel_details.\n"
        "  get_channel_details(channel_code) → channel config.\n"
        "ADVANCED:\n"
        "  call_unicommerce_soap(operation, payload_xml) → raw SOAP call for anything not covered above.\n\n"
        "COMPLEX QUERY STRATEGY:\n"
        "- 'How is business this week?' → get_order_summary(7) + get_fulfillment_health(7)\n"
        "- 'What is selling best?' → get_top_selling_skus(30)\n"
        "- 'Revenue trend this month?' → get_revenue_trend(30, 'day')\n"
        "- 'Which channel is best?' → get_channel_performance(30)\n"
        "- 'Stock situation?' → get_inventory_summary() + get_reorder_suggestions()\n"
        "- 'What needs reordering?' → get_reorder_suggestions() then get_vendor_insights() for who to call\n"
        "- 'Any expiry risk?' → get_expiry_alerts(90)\n"
        "- 'Vendor performance?' → get_vendor_insights(3)\n"
        "- 'Procurement for May?' → analyze_procurement_needs(5, 2026)\n"
        "- 'Delayed shipments?' → search_shipments(delayed_only=True) or get_fulfillment_health()\n\n"
        "FORMATTING RULES — always follow these:\n"
        "1. Lead with a bold summary line (e.g. **47 orders · ₹1,23,456 revenue · 3 delayed shipments**)\n"
        "2. Use markdown tables for any list with 3+ rows\n"
        "3. Use ₹ for INR with Indian comma formatting (₹1,23,456 not ₹123456)\n"
        "4. Show percentages for channel/status breakdowns\n"
        "5. Flag anomalies in bold: **OUT OF STOCK**, **DELAYED**, **CANCELLED**, **CRITICAL**\n"
        "6. Group by channel or warehouse when showing mixed data\n"
        "7. For dates, show in readable form: 'May 5, 2026' not ISO strings\n"
        "8. For trend data, describe direction: 'up 12% vs prior week' where possible\n"
        "9. Always call multiple tools if the question spans multiple data domains\n"
    ),
    stateless_http=True,
    json_response=True,
    streamable_http_path="/",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    ),
)


def _client() -> UnicommerceSoapClient:
    return UnicommerceSoapClient(settings)


def _serial(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _clean(d: dict) -> dict:
    return {k: _serial(v) for k, v in d.items() if k != "raw_payload" and v is not None and v != ""}


async def _fetch_receipts_parallel(
    client: UnicommerceSoapClient, codes: list[str], max_concurrent: int = 6
) -> list[dict]:
    """Fetch multiple inflow receipt details in parallel with a concurrency cap."""
    sem = asyncio.Semaphore(max_concurrent)

    async def _one(code: str) -> list[dict]:
        async with sem:
            try:
                r = await client.call("GetInflowReceiptDetail", build_get_inflow_receipt_detail_request(code))
                return normalize_inflow_receipt_detail(r.payload if isinstance(r.payload, dict) else {})
            except Exception as exc:
                logger.warning("Receipt detail %s failed: %s", code, exc)
                return []

    batches = await asyncio.gather(*[_one(c) for c in codes])
    return [receipt for batch in batches for receipt in batch]


async def _fetch_po_details_parallel(
    client: UnicommerceSoapClient, codes: list[str], max_concurrent: int = 6
) -> list[dict]:
    """Fetch multiple PO details in parallel with a concurrency cap."""
    sem = asyncio.Semaphore(max_concurrent)

    async def _one(code: str) -> dict:
        async with sem:
            try:
                r = await client.call("GetPurchaseOrderDetail", build_get_purchase_order_detail_request(code))
                return normalize_purchase_order_detail(r.payload if isinstance(r.payload, dict) else {})
            except Exception as exc:
                logger.warning("PO detail %s failed: %s", code, exc)
                return {}

    results = await asyncio.gather(*[_one(c) for c in codes])
    return [r for r in results if r]


async def _fetch_order_details_parallel(
    client: UnicommerceSoapClient, codes: list[str], max_concurrent: int = 6
) -> list[dict]:
    """Fetch multiple sale order details in parallel."""
    sem = asyncio.Semaphore(max_concurrent)

    async def _one(code: str) -> dict | None:
        async with sem:
            try:
                r = await client.call("GetSaleOrder", build_get_sale_order_request(code))
                return normalize_order(r.payload if isinstance(r.payload, dict) else {})
            except Exception as exc:
                logger.warning("Order detail %s failed: %s", code, exc)
                return None

    results = await asyncio.gather(*[_one(c) for c in codes])
    return [r for r in results if r]


@mcp.tool()
async def search_orders(
    from_date: str | None = None,
    to_date: str | None = None,
    updated_since_minutes: int | None = None,
    status: str | None = None,
    max_results: int = 50,
) -> list[dict]:
    """
    Search sale orders from Unicommerce.

    Args:
        from_date: ISO-8601 start date filter on order creation (e.g. "2026-01-01T00:00:00+00:00")
        to_date: ISO-8601 end date filter on order creation
        updated_since_minutes: fetch orders updated in the last N minutes (alternative to date range)
        status: filter by order status string (e.g. "CREATED", "DISPATCHED")
        max_results: cap on number of orders returned (default 50, max 500)
    """
    max_results = min(max_results, 500)
    client = _client()
    try:
        results: list[dict] = []
        page_size = min(max_results, settings.page_size)
        start = 0
        pages = 0
        while len(results) < max_results and pages < settings.max_pages:
            request = build_search_sale_order_request(
                start=start,
                length=page_size,
                from_date=datetime.fromisoformat(from_date) if from_date else None,
                to_date=datetime.fromisoformat(to_date) if to_date else None,
                updated_since_minutes=updated_since_minutes,
            )
            response = await client.call("SearchSaleOrder", request)
            payload = response.payload if isinstance(response.payload, dict) else {}
            total = int(payload.get("TotalRecords") or 0)
            raw_orders = payload.get("SaleOrders", {}).get("SaleOrder") if isinstance(payload.get("SaleOrders"), dict) else []
            if not isinstance(raw_orders, list):
                raw_orders = [raw_orders] if raw_orders else []

            for o in raw_orders:
                order_status = o.get("StatusCode") or o.get("Status") or ""
                if status and order_status.upper() != status.upper():
                    continue
                results.append({
                    "order_code": o.get("Code"),
                    "display_order_code": o.get("DisplayOrderCode"),
                    "channel": o.get("Channel"),
                    "status": order_status,
                    "amount_paid": o.get("AmountPaid"),
                    "created_on": o.get("CreatedOn"),
                    "updated_on": o.get("UpdatedOn"),
                })

            start += page_size
            pages += 1
            if start >= total or total == 0:
                break

        return results[:max_results]
    finally:
        await client.close()


@mcp.tool()
async def get_order(order_code: str) -> dict:
    """
    Get full detail for a single order: customer info, shipping address, line items,
    pricing breakdown, and any return/cancellation records inferred from item status.

    Args:
        order_code: Unicommerce order code (e.g. "SO12345")
    """
    client = _client()
    try:
        response = await client.call("GetSaleOrder", build_get_sale_order_request(order_code))
        payload = response.payload if isinstance(response.payload, dict) else {}
        normalized = normalize_order(payload)
        order = _clean(normalized["order"])
        items = []
        for item in normalized["items"]:
            d = _clean(item)
            d["facility_name"] = _facility_name(d.get("facility_code"))
            items.append(d)
        returns = [_clean(r) for r in normalized["returns"]]
        return {**order, "items": items, "returns": returns}
    finally:
        await client.close()


@mcp.tool()
async def search_shipments(
    created_from: str | None = None,
    created_to: str | None = None,
    updated_since_minutes: int | None = None,
    status: str | None = None,
    delayed_only: bool = False,
    max_results: int = 50,
) -> list[dict]:
    """
    Search shipping packages from Unicommerce.
    Each result includes tracking info and delivery_delay_hours
    (hours beyond the configured threshold; 0 means on time).

    Args:
        created_from: ISO-8601 start date for shipment creation
        created_to: ISO-8601 end date for shipment creation
        updated_since_minutes: fetch shipments updated in last N minutes
        status: filter by shipment status (e.g. "DISPATCHED", "DELIVERED")
        delayed_only: return only shipments with delivery_delay_hours > 0
        max_results: cap on results (default 50, max 500)
    """
    max_results = min(max_results, 500)
    client = _client()
    try:
        results: list[dict] = []
        page_size = min(max_results, settings.page_size)
        start = 0
        pages = 0
        while len(results) < max_results and pages < settings.max_pages:
            request = build_search_shipping_package_request(
                start=start,
                length=page_size,
                created_from=datetime.fromisoformat(created_from) if created_from else None,
                created_to=datetime.fromisoformat(created_to) if created_to else None,
                updated_since_minutes=updated_since_minutes,
            )
            response = await client.call("SearchShippingPackage", request)
            payload = response.payload if isinstance(response.payload, dict) else {}
            total = int(payload.get("TotalRecords") or 0)
            shipments = normalize_shipment_search_response(payload, settings.shipment_delay_threshold_hours)

            for s in shipments:
                if status and (s.get("status") or "").upper() != status.upper():
                    continue
                if delayed_only and not (s.get("delivery_delay_hours") or 0) > 0:
                    continue
                d = _clean(s)
                d["facility_name"] = _facility_name(d.get("facility_code"))
                results.append(d)

            start += page_size
            pages += 1
            if start >= total or total == 0:
                break

        return results[:max_results]
    finally:
        await client.close()


@mcp.tool()
async def get_inventory(
    sku: str | None = None,
    warehouse: str | None = None,
    low_stock_only: bool = False,
    max_results: int = 100,
) -> list[dict]:
    """
    Get current inventory levels per SKU per warehouse.
    Includes open_sale, open_purchase, putaway_pending, and inventory_blocked counts.

    Args:
        sku: filter to a specific SKU code
        warehouse: filter by facility code — "namhyafood" (NAMHYA FOODS DELHI) or "namhya_jammu" (NAMHYA FOODS JAMMU)
        low_stock_only: return only SKUs with inventory at or below the low-stock threshold
        max_results: cap on snapshot records returned (default 100, max 1000)
    """
    max_results = min(max_results, 1000)
    client = _client()
    try:
        results: list[dict] = []
        page_size = settings.page_size
        start = 0
        pages = 0
        while len(results) < max_results and pages < settings.max_pages:
            request = build_search_item_types_request(start=start, length=page_size)
            response = await client.call("SearchItemTypes", request)
            payload = response.payload if isinstance(response.payload, dict) else {}
            total = int(payload.get("TotalRecords") or 0)
            normalized = normalize_inventory_search_response(payload)

            for snapshot in normalized["inventory_snapshots"]:
                if sku and snapshot.get("sku_code") != sku:
                    continue
                if warehouse and snapshot.get("warehouse_code") != warehouse:
                    continue
                if low_stock_only and (snapshot.get("inventory") or 0) > settings.low_stock_threshold:
                    continue
                d = _clean(snapshot)
                d["warehouse_name"] = _facility_name(d.get("warehouse_code"))
                results.append(d)

            start += page_size
            pages += 1
            if start >= total or total == 0:
                break

        return results[:max_results]
    finally:
        await client.close()


@mcp.tool()
async def get_channel_details(channel_code: str) -> dict:
    """
    Get details for a specific Unicommerce sales channel (e.g. Amazon, Flipkart, website).

    Args:
        channel_code: the channel code as it appears in orders
    """
    client = _client()
    try:
        response = await client.call("GetChannelDetails", build_get_channel_details_request(channel_code))
        payload = response.payload if isinstance(response.payload, dict) else {}
        normalized = normalize_channel(payload)
        if normalized:
            return {k: v for k, v in normalized.items() if k != "raw_payload"}
        return {"error": f"No details found for channel '{channel_code}'"}
    finally:
        await client.close()


@mcp.tool()
async def get_order_summary(
    days: int = 7,
    max_orders: int = 500,
) -> dict:
    """
    Aggregated order snapshot: total count, revenue, breakdown by channel and status.
    Use this as the starting point for any business overview question.

    Args:
        days: look-back window in days (default 7)
        max_orders: max orders to analyse (default 500)
    """
    from datetime import timezone, timedelta
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=days)

    client = _client()
    try:
        results: list[dict] = []
        page_size = min(max_orders, settings.page_size)
        start = 0
        pages = 0
        while len(results) < max_orders and pages < settings.max_pages:
            request = build_search_sale_order_request(
                start=start, length=page_size,
                from_date=from_dt, to_date=to_dt,
            )
            response = await client.call("SearchSaleOrder", request)
            payload = response.payload if isinstance(response.payload, dict) else {}
            total = int(payload.get("TotalRecords") or 0)
            raw_orders = payload.get("SaleOrders", {}).get("SaleOrder") if isinstance(payload.get("SaleOrders"), dict) else []
            if not isinstance(raw_orders, list):
                raw_orders = [raw_orders] if raw_orders else []
            results.extend(raw_orders)
            start += page_size
            pages += 1
            if start >= total or total == 0:
                break
    finally:
        await client.close()

    by_status: dict[str, int] = {}
    by_channel: dict[str, dict] = {}
    total_revenue = 0.0

    for o in results:
        status = (o.get("StatusCode") or o.get("Status") or "UNKNOWN").upper()
        channel = o.get("Channel") or "UNKNOWN"
        amount = float(o.get("AmountPaid") or 0)

        by_status[status] = by_status.get(status, 0) + 1
        if channel not in by_channel:
            by_channel[channel] = {"orders": 0, "revenue": 0.0}
        by_channel[channel]["orders"] += 1
        by_channel[channel]["revenue"] += amount
        total_revenue += amount

    return {
        "period_days": days,
        "total_orders": len(results),
        "total_revenue_inr": round(total_revenue, 2),
        "by_status": dict(sorted(by_status.items(), key=lambda x: -x[1])),
        "by_channel": {
            ch: {"orders": v["orders"], "revenue_inr": round(v["revenue"], 2)}
            for ch, v in sorted(by_channel.items(), key=lambda x: -x[1]["orders"])
        },
    }


@mcp.tool()
async def get_inventory_summary(
    warehouse: str | None = None,
) -> dict:
    """
    Aggregated inventory snapshot: total SKUs, out-of-stock count, low-stock count,
    total units, and per-warehouse breakdown.

    Args:
        warehouse: optional filter — "namhyafood" (Delhi) or "namhya_jammu" (Jammu)
    """
    client = _client()
    try:
        all_skus: list[dict] = []
        all_snapshots: list[dict] = []
        page_size = settings.page_size
        start = 0
        pages = 0
        while pages < settings.max_pages:
            request = build_search_item_types_request(start=start, length=page_size)
            response = await client.call("SearchItemTypes", request)
            payload = response.payload if isinstance(response.payload, dict) else {}
            total = int(payload.get("TotalRecords") or 0)
            normalized = normalize_inventory_search_response(payload)
            all_skus.extend(normalized["skus"])
            all_snapshots.extend(normalized["inventory_snapshots"])
            start += page_size
            pages += 1
            if start >= total or total == 0:
                break
    finally:
        await client.close()

    if warehouse:
        all_snapshots = [s for s in all_snapshots if s.get("warehouse_code") == warehouse]

    # Deduplicate snapshots: keep highest inventory per (sku, warehouse)
    seen: dict[tuple, dict] = {}
    for s in all_snapshots:
        key = (s.get("sku_code"), s.get("warehouse_code"))
        if key not in seen or (s.get("inventory") or 0) > (seen[key].get("inventory") or 0):
            seen[key] = s
    snapshots = list(seen.values())

    # Unique SKU codes (regardless of warehouse or inventory)
    unique_skus = {s.get("sku_code") for s in all_skus if s.get("sku_code")}
    skus_with_inventory = {s.get("sku_code") for s in snapshots if (s.get("inventory") or 0) > 0}
    skus_no_inventory = unique_skus - skus_with_inventory

    out_of_stock = [s for s in snapshots if (s.get("inventory") or 0) == 0]
    low_stock = [s for s in snapshots if 0 < (s.get("inventory") or 0) <= settings.low_stock_threshold]
    by_warehouse: dict[str, dict] = {}
    for s in snapshots:
        wcode = s.get("warehouse_code") or "UNKNOWN"
        wname = _facility_name(wcode) or wcode
        if wname not in by_warehouse:
            by_warehouse[wname] = {"skus_with_stock": 0, "total_units": 0, "out_of_stock": 0, "low_stock": 0}
        inv = s.get("inventory") or 0
        by_warehouse[wname]["total_units"] += inv
        if inv == 0:
            by_warehouse[wname]["out_of_stock"] += 1
        elif inv <= settings.low_stock_threshold:
            by_warehouse[wname]["low_stock"] += 1
            by_warehouse[wname]["skus_with_stock"] += 1
        else:
            by_warehouse[wname]["skus_with_stock"] += 1

    return {
        "total_skus_in_unicommerce": len(unique_skus),
        "skus_with_stock": len(skus_with_inventory),
        "skus_out_of_stock": len(skus_no_inventory),
        "low_stock_skus": len(low_stock),
        "total_units_across_warehouses": sum(s.get("inventory") or 0 for s in snapshots),
        "by_warehouse": by_warehouse,
        "out_of_stock_list": [
            {"sku": s.get("sku_code"), "name": s.get("product_name"), "warehouse": _facility_name(s.get("warehouse_code"))}
            for s in out_of_stock[:20]
        ],
        "low_stock_list": [
            {"sku": s.get("sku_code"), "name": s.get("product_name"), "warehouse": _facility_name(s.get("warehouse_code")), "units": s.get("inventory")}
            for s in sorted(low_stock, key=lambda x: x.get("inventory") or 0)[:20]
        ],
    }


@mcp.tool()
async def get_purchase_orders(
    from_date: str | None = None,
    to_date: str | None = None,
    vendor_name: str | None = None,
) -> list[str]:
    """
    Get list of purchase order codes from Unicommerce.
    Use get_purchase_order_detail to drill into a specific PO.

    Args:
        from_date: ISO-8601 start of creation date range
        to_date: ISO-8601 end of creation date range
        vendor_name: optional filter by vendor name
    """
    client = _client()
    try:
        request = build_get_purchase_orders_request(
            from_date=datetime.fromisoformat(from_date) if from_date else None,
            to_date=datetime.fromisoformat(to_date) if to_date else None,
            vendor_name=vendor_name,
        )
        response = await client.call("GetPurchaseOrders", request)
        payload = response.payload if isinstance(response.payload, dict) else {}
        return normalize_purchase_orders_response(payload)
    finally:
        await client.close()


@mcp.tool()
async def get_purchase_order_detail(po_code: str) -> dict:
    """
    Get full detail for a purchase order including all line items (SKUs, quantities, prices).

    Args:
        po_code: purchase order code (e.g. "PO-1234")
    """
    client = _client()
    try:
        response = await client.call("GetPurchaseOrderDetail", build_get_purchase_order_detail_request(po_code))
        payload = response.payload if isinstance(response.payload, dict) else {}
        detail = normalize_purchase_order_detail(payload)
        items = [_clean(i) for i in detail.pop("items", [])]
        return {**_clean(detail), "items": items}
    finally:
        await client.close()


@mcp.tool()
async def get_inflow_receipts(
    from_date: str | None = None,
    to_date: str | None = None,
    po_code: str | None = None,
) -> list[str]:
    """
    Get list of goods receipt (inflow receipt) codes — what has actually arrived at the warehouse.
    Use get_inflow_receipt_detail for full item-level breakdown.

    Args:
        from_date: ISO-8601 start of creation date range
        to_date: ISO-8601 end of creation date range
        po_code: filter receipts linked to a specific purchase order
    """
    client = _client()
    try:
        request = build_get_inflow_receipts_request(
            po_code=po_code,
            from_date=datetime.fromisoformat(from_date) if from_date else None,
            to_date=datetime.fromisoformat(to_date) if to_date else None,
        )
        response = await client.call("GetInflowReceipts", request)
        payload = response.payload if isinstance(response.payload, dict) else {}
        return normalize_inflow_receipts_response(payload)
    finally:
        await client.close()


@mcp.tool()
async def get_inflow_receipt_detail(receipt_code: str) -> list[dict]:
    """
    Get full detail of a goods receipt (GRN) — items received, quantities, batch/expiry info.

    Args:
        receipt_code: inflow receipt code
    """
    client = _client()
    try:
        response = await client.call("GetInflowReceiptDetail", build_get_inflow_receipt_detail_request(receipt_code))
        payload = response.payload if isinstance(response.payload, dict) else {}
        return [_clean(r) for r in normalize_inflow_receipt_detail(payload)]
    finally:
        await client.close()


@mcp.tool()
async def get_back_order_items() -> list[dict]:
    """
    Get items that are in backorder — customer orders exist but inventory is insufficient to fulfil them.
    These SKUs need urgent restocking.
    """
    client = _client()
    try:
        response = await client.call("GetBackOrderItems", build_get_back_order_items_request())
        payload = response.payload if isinstance(response.payload, dict) else {}
        return [_clean(item) for item in normalize_back_order_items(payload)]
    finally:
        await client.close()


@mcp.tool()
async def get_vendors(
    from_date: str | None = None,
    to_date: str | None = None,
) -> list[dict]:
    """
    Get list of vendors/suppliers registered in Unicommerce.

    Args:
        from_date: ISO-8601 start date (defaults to 1 year ago)
        to_date: ISO-8601 end date (defaults to today)
    """
    from datetime import timezone, timedelta
    now = datetime.now(timezone.utc)
    client = _client()
    try:
        request = build_get_vendors_request(
            from_date=datetime.fromisoformat(from_date) if from_date else now - timedelta(days=365),
            to_date=datetime.fromisoformat(to_date) if to_date else now,
        )
        response = await client.call("GetVendors", request)
        payload = response.payload if isinstance(response.payload, dict) else {}
        return [_clean(v) for v in normalize_vendors(payload)]
    finally:
        await client.close()


@mcp.tool()
async def get_vendor_item_types(page: int = 1, page_size: int = 100) -> dict:
    """
    Get SKUs mapped to vendors — shows which supplier provides which product.

    Args:
        page: page number (default 1)
        page_size: results per page (default 100, max 200)
    """
    client = _client()
    try:
        request = build_get_vendor_item_types_request(page_number=page, page_size=min(page_size, 200))
        response = await client.call("GetVendorItemTypes", request)
        payload = response.payload if isinstance(response.payload, dict) else {}
        result = normalize_vendor_item_types(payload)
        result["items"] = [_clean(i) for i in result["items"]]
        return result
    finally:
        await client.close()


@mcp.tool()
async def analyze_procurement_needs(
    target_month: int,
    target_year: int,
    reference_months: int = 1,
) -> dict:
    """
    Procurement planning: how much of each SKU needs to be ordered for the target month.
    Uses purchase order history from the reference period as the demand baseline,
    then subtracts current inventory to produce a suggested order quantity.

    Example: target_month=5, target_year=2026 → looks at April 2026 POs as baseline,
    fetches current stock, and returns what needs to be ordered for May.

    Args:
        target_month: month to plan for (1-12)
        target_year: year to plan for (e.g. 2026)
        reference_months: how many prior months to average demand over (default 1)
    """
    from datetime import timezone
    import calendar

    # Calculate reference period (months before target)
    ref_end_month = target_month - 1 if target_month > 1 else 12
    ref_end_year = target_year if target_month > 1 else target_year - 1
    ref_start_month = ref_end_month - reference_months + 1
    ref_start_year = ref_end_year
    if ref_start_month < 1:
        ref_start_month += 12
        ref_start_year -= 1

    ref_start = datetime(ref_start_year, ref_start_month, 1, tzinfo=timezone.utc)
    last_day = calendar.monthrange(ref_end_year, ref_end_month)[1]
    ref_end = datetime(ref_end_year, ref_end_month, last_day, 23, 59, 59, tzinfo=timezone.utc)

    client = _client()
    try:
        # Step 1: get PO codes for the reference period
        po_response = await client.call(
            "GetPurchaseOrders",
            build_get_purchase_orders_request(from_date=ref_start, to_date=ref_end),
        )
        po_payload = po_response.payload if isinstance(po_response.payload, dict) else {}
        po_codes = normalize_purchase_orders_response(po_payload)

        # Step 2: fetch detail for each PO (cap at 30 to avoid timeout)
        sku_demand: dict[str, dict] = {}
        for po_code in po_codes[:30]:
            detail_response = await client.call(
                "GetPurchaseOrderDetail",
                build_get_purchase_order_detail_request(po_code),
            )
            detail_payload = detail_response.payload if isinstance(detail_response.payload, dict) else {}
            po_detail = normalize_purchase_order_detail(detail_payload)
            for item in po_detail.get("items") or []:
                sku = item.get("sku_code")
                if not sku:
                    continue
                if sku not in sku_demand:
                    sku_demand[sku] = {
                        "sku_code": sku,
                        "total_ordered_in_reference": 0,
                        "unit_price": float(item.get("unit_price") or 0),
                        "po_codes": [],
                    }
                sku_demand[sku]["total_ordered_in_reference"] += item.get("quantity") or 0
                sku_demand[sku]["po_codes"].append(po_code)

        # Step 3: fetch current inventory for these SKUs
        inv_response = await client.call(
            "SearchItemTypes",
            build_search_item_types_request(start=0, length=settings.page_size),
        )
        inv_payload = inv_response.payload if isinstance(inv_response.payload, dict) else {}
        inv_total = int(inv_payload.get("TotalRecords") or 0)
        all_inv_pages = [normalize_inventory_search_response(inv_payload)]

        page_start = settings.page_size
        pages = 1
        while page_start < inv_total and pages < settings.max_pages:
            r = await client.call(
                "SearchItemTypes",
                build_search_item_types_request(start=page_start, length=settings.page_size),
            )
            p = r.payload if isinstance(r.payload, dict) else {}
            all_inv_pages.append(normalize_inventory_search_response(p))
            page_start += settings.page_size
            pages += 1

        current_stock: dict[str, int] = {}
        for page_data in all_inv_pages:
            for snap in page_data["inventory_snapshots"]:
                sku = snap.get("sku_code")
                if sku:
                    current_stock[sku] = current_stock.get(sku, 0) + (snap.get("inventory") or 0)

    finally:
        await client.close()

    # Step 4: build procurement plan
    monthly_demand = {
        sku: round(data["total_ordered_in_reference"] / reference_months)
        for sku, data in sku_demand.items()
    }

    plan = []
    for sku, demand in sorted(monthly_demand.items(), key=lambda x: -x[1]):
        stock = current_stock.get(sku, 0)
        suggested_order = max(0, demand - stock)
        plan.append({
            "sku_code": sku,
            "monthly_demand_units": demand,
            "current_stock_units": stock,
            "suggested_order_units": suggested_order,
            "unit_price_inr": sku_demand[sku]["unit_price"],
            "estimated_order_value_inr": round(suggested_order * sku_demand[sku]["unit_price"], 2),
            "stock_status": "OK" if stock >= demand else ("LOW" if stock > 0 else "OUT OF STOCK"),
        })

    total_order_value = sum(row["estimated_order_value_inr"] for row in plan)
    skus_needing_order = [r for r in plan if r["suggested_order_units"] > 0]

    return {
        "reference_period": f"{ref_start.strftime('%b %Y')} – {ref_end.strftime('%b %Y')}",
        "target_month": f"{datetime(target_year, target_month, 1).strftime('%B %Y')}",
        "purchase_orders_analysed": len(po_codes[:30]),
        "total_skus_in_pos": len(plan),
        "skus_needing_reorder": len(skus_needing_order),
        "total_estimated_order_value_inr": round(total_order_value, 2),
        "procurement_plan": plan,
    }


@mcp.tool()
async def get_inventory_insights(days: int = 90) -> dict:
    """
    Full inventory intelligence per SKU: current stock, days of stock remaining,
    stock valuation in INR, and consumption rate based on inflow history.

    Args:
        days: lookback window for consumption calculation (default 90)
    """
    from datetime import timezone, timedelta
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=days)

    client = _client()
    try:
        # 1. Fetch all inventory snapshots
        all_snapshots: list[dict] = []
        all_skus: list[dict] = []
        start, pages = 0, 0
        while pages < settings.max_pages:
            r = await client.call("SearchItemTypes", build_search_item_types_request(start=start, length=settings.page_size))
            p = r.payload if isinstance(r.payload, dict) else {}
            total = int(p.get("TotalRecords") or 0)
            norm = normalize_inventory_search_response(p)
            all_snapshots.extend(norm["inventory_snapshots"])
            all_skus.extend(norm["skus"])
            start += settings.page_size
            pages += 1
            if start >= total or total == 0:
                break

        # 2. Fetch inflow receipts for consumption proxy (parallel, cap 150)
        r = await client.call("GetInflowReceipts", build_get_inflow_receipts_request(from_date=since, to_date=now))
        receipt_codes = normalize_inflow_receipts_response(r.payload if isinstance(r.payload, dict) else {})
        all_receipts = await _fetch_receipts_parallel(client, receipt_codes[:150])

        sku_inflow: dict[str, int] = {}
        for receipt in all_receipts:
            for item in receipt.get("items") or []:
                sku = item.get("sku_code")
                qty = item.get("quantity_received") or 0
                if sku:
                    sku_inflow[sku] = sku_inflow.get(sku, 0) + qty

        # 3. Fetch vendor pricing + names (all pages in parallel)
        vit_r = await client.call("GetVendorItemTypes", build_get_vendor_item_types_request(page_number=1, page_size=200))
        vit = normalize_vendor_item_types(vit_r.payload if isinstance(vit_r.payload, dict) else {})
        sku_price: dict[str, float] = {i["sku_code"]: float(i.get("unit_price") or 0) for i in vit["items"] if i.get("sku_code")}
        sku_vendor: dict[str, str] = {i["sku_code"]: i.get("vendor_code", "") for i in vit["items"] if i.get("sku_code")}

    finally:
        await client.close()

    # 4. Deduplicate snapshots: aggregate stock per SKU with per-warehouse breakdown
    stock_by_sku: dict[str, dict] = {}
    for s in all_snapshots:
        sku = s.get("sku_code")
        if not sku:
            continue
        if sku not in stock_by_sku:
            stock_by_sku[sku] = {"total_stock": 0, "open_sale": 0, "warehouses": {}}
        wcode = s.get("warehouse_code") or "UNKNOWN"
        wname = _facility_name(wcode) or wcode
        stock_by_sku[sku]["total_stock"] += s.get("inventory") or 0
        stock_by_sku[sku]["open_sale"] += s.get("open_sale") or 0
        if wname not in stock_by_sku[sku]["warehouses"]:
            stock_by_sku[sku]["warehouses"][wname] = {"stock": 0, "open_sale": 0, "open_purchase": 0}
        stock_by_sku[sku]["warehouses"][wname]["stock"] += s.get("inventory") or 0
        stock_by_sku[sku]["warehouses"][wname]["open_sale"] += s.get("open_sale") or 0
        stock_by_sku[sku]["warehouses"][wname]["open_purchase"] += s.get("open_purchase") or 0

    sku_name: dict[str, str] = {s.get("sku_code", ""): s.get("name", "") for s in all_skus}

    daily_consumption = {sku: qty / days for sku, qty in sku_inflow.items()}

    insights = []
    for sku, stock_data in stock_by_sku.items():
        stock = stock_data["total_stock"]
        daily = daily_consumption.get(sku, 0)
        days_left = round(stock / daily) if daily > 0 else None
        price = sku_price.get(sku, 0)
        insights.append({
            "sku_code": sku,
            "product_name": sku_name.get(sku),
            "vendor_code": sku_vendor.get(sku),
            "total_stock_units": stock,
            "open_sale_units": stock_data["open_sale"],
            "available_units": stock - stock_data["open_sale"],
            "days_of_stock_remaining": days_left,
            "stock_status": (
                "OUT OF STOCK" if stock == 0 else
                "CRITICAL" if days_left is not None and days_left <= 15 else
                "LOW" if days_left is not None and days_left <= 45 else
                "NO DATA" if days_left is None else "OK"
            ),
            "avg_daily_consumption_units": round(daily, 2) if daily > 0 else None,
            "monthly_consumption_units": round(daily * 30, 1) if daily > 0 else None,
            "stock_value_inr": round(stock * price, 2) if price > 0 else None,
            "unit_price_inr": price if price > 0 else None,
            "by_warehouse": [
                {"warehouse": wname, **wdata}
                for wname, wdata in stock_data["warehouses"].items()
            ],
        })

    insights.sort(key=lambda x: (
        0 if x["stock_status"] == "OUT OF STOCK" else
        1 if x["stock_status"] == "CRITICAL" else
        2 if x["stock_status"] == "LOW" else
        3 if x["stock_status"] == "NO DATA" else 4
    ))

    total_value = sum(i["stock_value_inr"] or 0 for i in insights)
    return {
        "analysis_period_days": days,
        "total_skus": len(insights),
        "out_of_stock": sum(1 for i in insights if i["stock_status"] == "OUT OF STOCK"),
        "critical_stock": sum(1 for i in insights if i["stock_status"] == "CRITICAL"),
        "low_stock": sum(1 for i in insights if i["stock_status"] == "LOW"),
        "total_inventory_value_inr": round(total_value, 2),
        "skus": insights,
    }


@mcp.tool()
async def get_expiry_alerts(alert_days: int = 90) -> dict:
    """
    Batch and expiry tracking: identifies stock received in GRNs that is expiring
    within the next alert_days days. Critical for food product quality management.

    Args:
        alert_days: flag batches expiring within this many days (default 90)
    """
    from datetime import timezone, timedelta, date
    now = datetime.now(timezone.utc).date()
    alert_cutoff = now + timedelta(days=alert_days)

    client = _client()
    try:
        # Fetch receipts from last 18 months (food typically has < 2yr shelf life)
        since = datetime.now(timezone.utc) - timedelta(days=548)
        r = await client.call("GetInflowReceipts", build_get_inflow_receipts_request(
            from_date=since, to_date=datetime.now(timezone.utc)
        ))
        receipt_codes = normalize_inflow_receipts_response(r.payload if isinstance(r.payload, dict) else {})

        # Parallel fetch, cap 150
        all_receipts = await _fetch_receipts_parallel(client, receipt_codes[:150])
        alerts = []
        for receipt in all_receipts:
            for item in receipt.get("items") or []:
                expiry_raw = item.get("expiry_date")
                if not expiry_raw:
                    continue
                try:
                    expiry = date.fromisoformat(str(expiry_raw)[:10])
                except Exception:
                    continue
                if expiry > alert_cutoff:
                    continue
                days_until = (expiry - now).days
                alerts.append({
                    "sku_code": item.get("sku_code"),
                    "product_name": item.get("product_name"),
                    "batch_code": item.get("batch_code"),
                    "expiry_date": str(expiry),
                    "days_until_expiry": days_until,
                    "urgency": "EXPIRED" if days_until < 0 else "CRITICAL" if days_until <= 30 else "WARNING" if days_until <= 60 else "MONITOR",
                    "quantity_received": item.get("quantity_received"),
                    "receipt_code": receipt.get("receipt_code"),
                    "vendor_name": receipt.get("vendor_name"),
                    "receipt_date": receipt.get("created"),
                })
    finally:
        await client.close()

    alerts.sort(key=lambda x: x["days_until_expiry"])
    return {
        "alert_window_days": alert_days,
        "total_alerts": len(alerts),
        "expired": sum(1 for a in alerts if a["urgency"] == "EXPIRED"),
        "critical": sum(1 for a in alerts if a["urgency"] == "CRITICAL"),
        "warning": sum(1 for a in alerts if a["urgency"] == "WARNING"),
        "monitor": sum(1 for a in alerts if a["urgency"] == "MONITOR"),
        "alerts": alerts,
    }


@mcp.tool()
async def get_vendor_insights(months: int = 3) -> dict:
    """
    Vendor performance analysis: what each vendor supplies, procurement history,
    fill rate, and single-vendor dependency risk alerts.

    Args:
        months: lookback window for procurement history (default 3)
    """
    from datetime import timezone, timedelta
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=months * 30)

    client = _client()
    try:
        # Vendors list
        vr = await client.call("GetVendors", build_get_vendors_request(from_date=since, to_date=now))
        vendors = normalize_vendors(vr.payload if isinstance(vr.payload, dict) else {})
        vendor_map = {v["vendor_code"]: v for v in vendors if v.get("vendor_code")}

        # Vendor-SKU mapping (all pages)
        all_vit: list[dict] = []
        page = 1
        while True:
            vit_r = await client.call("GetVendorItemTypes", build_get_vendor_item_types_request(page_number=page, page_size=200))
            vit = normalize_vendor_item_types(vit_r.payload if isinstance(vit_r.payload, dict) else {})
            all_vit.extend(vit["items"])
            total = vit.get("total_records") or 0
            if page * 200 >= total or not vit["items"]:
                break
            page += 1

        # PO history
        po_r = await client.call("GetPurchaseOrders", build_get_purchase_orders_request(from_date=since, to_date=now))
        po_codes = normalize_purchase_orders_response(po_r.payload if isinstance(po_r.payload, dict) else {})

        # PO + receipt details fetched in parallel (cap 100 each)
        po_details = await _fetch_po_details_parallel(client, po_codes[:100])

        vendor_po: dict[str, dict] = {}
        for detail in po_details:
            vc = detail.get("vendor_code") or "UNKNOWN"
            if vc not in vendor_po:
                vendor_po[vc] = {"po_count": 0, "total_units_ordered": 0, "total_value_inr": 0.0, "skus_ordered": set()}
            vendor_po[vc]["po_count"] += 1
            for item in detail.get("items") or []:
                qty = item.get("quantity") or 0
                price = float(item.get("unit_price") or 0)
                vendor_po[vc]["total_units_ordered"] += qty
                vendor_po[vc]["total_value_inr"] += qty * price
                if item.get("sku_code"):
                    vendor_po[vc]["skus_ordered"].add(item["sku_code"])

        # Inflow receipts (actual received, parallel)
        ir_r = await client.call("GetInflowReceipts", build_get_inflow_receipts_request(from_date=since, to_date=now))
        receipt_codes = normalize_inflow_receipts_response(ir_r.payload if isinstance(ir_r.payload, dict) else {})
        all_receipts = await _fetch_receipts_parallel(client, receipt_codes[:100])
        vendor_received: dict[str, int] = {}
        for receipt in all_receipts:
            vc = receipt.get("vendor_code") or "UNKNOWN"
            for item in receipt.get("items") or []:
                vendor_received[vc] = vendor_received.get(vc, 0) + (item.get("quantity_received") or 0)

    finally:
        await client.close()

    # SKU → vendor mapping (which SKUs only have one vendor = dependency risk)
    sku_vendors: dict[str, list[str]] = {}
    for item in all_vit:
        sku = item.get("sku_code")
        vc = item.get("vendor_code")
        if sku and vc:
            sku_vendors.setdefault(sku, []).append(vc)

    single_vendor_skus = [sku for sku, vcs in sku_vendors.items() if len(vcs) == 1]

    result_vendors = []
    all_vendor_codes = set(vendor_map.keys()) | set(vendor_po.keys())
    for vc in all_vendor_codes:
        vinfo = vendor_map.get(vc, {})
        po_data = vendor_po.get(vc, {})
        ordered = po_data.get("total_units_ordered", 0)
        received = vendor_received.get(vc, 0)
        skus_supplied = [i["sku_code"] for i in all_vit if i.get("vendor_code") == vc and i.get("sku_code")]
        result_vendors.append({
            "vendor_code": vc,
            "vendor_name": vinfo.get("name") or vc,
            "email": vinfo.get("email"),
            "phone": vinfo.get("phone"),
            "city": vinfo.get("city"),
            "gst_number": vinfo.get("gst_number"),
            "skus_supplied": skus_supplied,
            "skus_count": len(skus_supplied),
            "po_count": po_data.get("po_count", 0),
            "total_units_ordered": ordered,
            "total_units_received": received,
            "fill_rate_pct": round(received / ordered * 100, 1) if ordered > 0 else None,
            "total_order_value_inr": round(po_data.get("total_value_inr", 0), 2),
        })

    result_vendors.sort(key=lambda x: -x["total_order_value_inr"])

    return {
        "period_months": months,
        "total_vendors": len(result_vendors),
        "single_vendor_sku_count": len(single_vendor_skus),
        "single_vendor_skus": single_vendor_skus[:20],
        "vendors": result_vendors,
    }


@mcp.tool()
async def get_reorder_suggestions(reorder_threshold_days: int = 45) -> dict:
    """
    Data-driven reorder recommendations: SKUs running low based on consumption
    rate vs current stock. Includes vendor, suggested quantity, and estimated cost.

    Args:
        reorder_threshold_days: flag SKUs with fewer than this many days of stock (default 45)
    """
    from datetime import timezone, timedelta
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=90)

    client = _client()
    try:
        # Current inventory
        stock_by_sku: dict[str, int] = {}
        open_sale_by_sku: dict[str, int] = {}
        sku_name: dict[str, str] = {}
        start, pages = 0, 0
        while pages < settings.max_pages:
            r = await client.call("SearchItemTypes", build_search_item_types_request(start=start, length=settings.page_size))
            p = r.payload if isinstance(r.payload, dict) else {}
            total = int(p.get("TotalRecords") or 0)
            norm = normalize_inventory_search_response(p)
            for s in norm["inventory_snapshots"]:
                sku = s.get("sku_code")
                if sku:
                    stock_by_sku[sku] = stock_by_sku.get(sku, 0) + (s.get("inventory") or 0)
                    open_sale_by_sku[sku] = open_sale_by_sku.get(sku, 0) + (s.get("open_sale") or 0)
            for s in norm["skus"]:
                if s.get("sku_code"):
                    sku_name[s["sku_code"]] = s.get("name", "")
            start += settings.page_size
            pages += 1
            if start >= total or total == 0:
                break

        # Consumption from inflow receipts (parallel, cap 150)
        ir_r = await client.call("GetInflowReceipts", build_get_inflow_receipts_request(from_date=since, to_date=now))
        receipt_codes = normalize_inflow_receipts_response(ir_r.payload if isinstance(ir_r.payload, dict) else {})
        all_receipts = await _fetch_receipts_parallel(client, receipt_codes[:150])
        sku_inflow: dict[str, int] = {}
        for receipt in all_receipts:
            for item in receipt.get("items") or []:
                sku = item.get("sku_code")
                if sku:
                    sku_inflow[sku] = sku_inflow.get(sku, 0) + (item.get("quantity_received") or 0)

        # Vendor pricing + mapping
        vit_r = await client.call("GetVendorItemTypes", build_get_vendor_item_types_request(page_number=1, page_size=200))
        vit = normalize_vendor_item_types(vit_r.payload if isinstance(vit_r.payload, dict) else {})
        sku_price: dict[str, float] = {}
        sku_vendor_code: dict[str, str] = {}
        for i in vit["items"]:
            sku = i.get("sku_code")
            if sku:
                sku_price[sku] = float(i.get("unit_price") or 0)
                sku_vendor_code[sku] = i.get("vendor_code", "")

        vr = await client.call("GetVendors", build_get_vendors_request(from_date=since, to_date=now))
        vendors = {v["vendor_code"]: v for v in normalize_vendors(vr.payload if isinstance(vr.payload, dict) else {}) if v.get("vendor_code")}

    finally:
        await client.close()

    suggestions = []
    for sku, stock in stock_by_sku.items():
        inflow_90d = sku_inflow.get(sku, 0)
        daily_consumption = inflow_90d / 90 if inflow_90d > 0 else None
        days_left = round(stock / daily_consumption) if daily_consumption and daily_consumption > 0 else None

        if days_left is None and stock == 0:
            urgency = "OUT OF STOCK"
        elif days_left is not None and days_left <= reorder_threshold_days:
            urgency = "REORDER NOW" if days_left <= 15 else "REORDER SOON"
        else:
            continue  # skip items with enough stock or no data

        monthly = round(daily_consumption * 30) if daily_consumption else None
        suggested_qty = max(0, (monthly * 2) - stock) if monthly else None
        vc = sku_vendor_code.get(sku)
        vendor = vendors.get(vc, {}) if vc else {}
        price = sku_price.get(sku, 0)

        suggestions.append({
            "sku_code": sku,
            "product_name": sku_name.get(sku),
            "urgency": urgency,
            "current_stock_units": stock,
            "open_sale_units": open_sale_by_sku.get(sku, 0),
            "days_of_stock_remaining": days_left,
            "avg_monthly_consumption_units": monthly,
            "suggested_order_quantity": suggested_qty,
            "estimated_order_value_inr": round(suggested_qty * price, 2) if suggested_qty and price else None,
            "unit_price_inr": price if price else None,
            "vendor_code": vc,
            "vendor_name": vendor.get("name"),
            "vendor_phone": vendor.get("phone"),
            "vendor_email": vendor.get("email"),
        })

    suggestions.sort(key=lambda x: (
        0 if x["urgency"] == "OUT OF STOCK" else
        1 if x["urgency"] == "REORDER NOW" else 2,
        x["days_of_stock_remaining"] or 999
    ))

    total_order_value = sum(s["estimated_order_value_inr"] or 0 for s in suggestions)
    return {
        "reorder_threshold_days": reorder_threshold_days,
        "skus_needing_reorder": len(suggestions),
        "out_of_stock": sum(1 for s in suggestions if s["urgency"] == "OUT OF STOCK"),
        "reorder_now": sum(1 for s in suggestions if s["urgency"] == "REORDER NOW"),
        "reorder_soon": sum(1 for s in suggestions if s["urgency"] == "REORDER SOON"),
        "total_estimated_order_value_inr": round(total_order_value, 2),
        "suggestions": suggestions,
    }


@mcp.tool()
async def list_channels(days: int = 30) -> list[dict]:
    """
    List all sales channels active in Unicommerce, with order count and total revenue.
    Use channel codes returned here as input to get_channel_details.

    Args:
        days: look-back window to detect active channels (default 30)
    """
    from datetime import timezone, timedelta
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=days)
    client = _client()
    try:
        results: list[dict] = []
        page_size = settings.page_size
        start = 0
        pages = 0
        while pages < settings.max_pages:
            r = await client.call(
                "SearchSaleOrder",
                build_search_sale_order_request(start=start, length=page_size, from_date=from_dt, to_date=to_dt),
            )
            p = r.payload if isinstance(r.payload, dict) else {}
            total = int(p.get("TotalRecords") or 0)
            raw = p.get("SaleOrders", {}).get("SaleOrder") if isinstance(p.get("SaleOrders"), dict) else []
            if not isinstance(raw, list):
                raw = [raw] if raw else []
            results.extend(raw)
            start += page_size
            pages += 1
            if start >= total or total == 0:
                break
    finally:
        await client.close()

    channels: dict[str, dict] = {}
    for o in results:
        ch = o.get("Channel") or "UNKNOWN"
        if ch not in channels:
            channels[ch] = {"channel_code": ch, "orders": 0, "revenue_inr": 0.0}
        channels[ch]["orders"] += 1
        channels[ch]["revenue_inr"] += float(o.get("AmountPaid") or 0)

    return sorted(
        [{"channel_code": k, "orders": v["orders"], "revenue_inr": round(v["revenue_inr"], 2)} for k, v in channels.items()],
        key=lambda x: -x["orders"],
    )


@mcp.tool()
async def get_revenue_trend(
    days: int = 30,
    group_by: str = "day",
) -> dict:
    """
    Revenue and order count trend over time — day-by-day or week-by-week breakdown.
    Use this to answer questions like "how has sales changed over the last month?"

    Args:
        days: look-back window (default 30, max 180)
        group_by: "day" or "week" (default "day"; use "week" for periods > 60 days)
    """
    from datetime import timezone, timedelta
    days = min(days, 180)
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=days)

    client = _client()
    try:
        results: list[dict] = []
        page_size = settings.page_size
        start = 0
        pages = 0
        while pages < settings.max_pages:
            r = await client.call(
                "SearchSaleOrder",
                build_search_sale_order_request(start=start, length=page_size, from_date=from_dt, to_date=to_dt),
            )
            p = r.payload if isinstance(r.payload, dict) else {}
            total = int(p.get("TotalRecords") or 0)
            raw = p.get("SaleOrders", {}).get("SaleOrder") if isinstance(p.get("SaleOrders"), dict) else []
            if not isinstance(raw, list):
                raw = [raw] if raw else []
            results.extend(raw)
            start += page_size
            pages += 1
            if start >= total or total == 0:
                break
    finally:
        await client.close()

    from collections import defaultdict
    buckets: dict[str, dict] = defaultdict(lambda: {"orders": 0, "revenue_inr": 0.0, "cancelled": 0})

    for o in results:
        raw_date = o.get("CreatedOn") or o.get("DisplayOrderDateTime") or ""
        try:
            dt = datetime.fromisoformat(raw_date[:19])
        except Exception:
            continue
        if group_by == "week":
            # ISO week key: YYYY-Www
            key = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
        else:
            key = str(dt.date())

        buckets[key]["orders"] += 1
        buckets[key]["revenue_inr"] += float(o.get("AmountPaid") or 0)
        status = (o.get("StatusCode") or o.get("Status") or "").upper()
        if "CANCEL" in status:
            buckets[key]["cancelled"] += 1

    trend = sorted(
        [
            {
                "period": k,
                "orders": v["orders"],
                "revenue_inr": round(v["revenue_inr"], 2),
                "cancelled_orders": v["cancelled"],
            }
            for k, v in buckets.items()
        ],
        key=lambda x: x["period"],
    )

    total_rev = sum(t["revenue_inr"] for t in trend)
    total_orders = sum(t["orders"] for t in trend)
    peak = max(trend, key=lambda x: x["revenue_inr"]) if trend else {}

    return {
        "period_days": days,
        "group_by": group_by,
        "total_orders": total_orders,
        "total_revenue_inr": round(total_rev, 2),
        "avg_daily_revenue_inr": round(total_rev / days, 2) if days else 0,
        "peak_period": peak,
        "trend": trend,
    }


@mcp.tool()
async def get_channel_performance(days: int = 30) -> dict:
    """
    Compare performance across all sales channels: orders, revenue, average order value,
    cancellation rate, and channel share. Useful for deciding where to focus marketing.

    Args:
        days: look-back window in days (default 30)
    """
    from datetime import timezone, timedelta
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=days)

    client = _client()
    try:
        results: list[dict] = []
        page_size = settings.page_size
        start = 0
        pages = 0
        while pages < settings.max_pages:
            r = await client.call(
                "SearchSaleOrder",
                build_search_sale_order_request(start=start, length=page_size, from_date=from_dt, to_date=to_dt),
            )
            p = r.payload if isinstance(r.payload, dict) else {}
            total = int(p.get("TotalRecords") or 0)
            raw = p.get("SaleOrders", {}).get("SaleOrder") if isinstance(p.get("SaleOrders"), dict) else []
            if not isinstance(raw, list):
                raw = [raw] if raw else []
            results.extend(raw)
            start += page_size
            pages += 1
            if start >= total or total == 0:
                break
    finally:
        await client.close()

    channels: dict[str, dict] = {}
    for o in results:
        ch = o.get("Channel") or "UNKNOWN"
        if ch not in channels:
            channels[ch] = {"orders": 0, "revenue": 0.0, "cancelled": 0, "cod_orders": 0}
        channels[ch]["orders"] += 1
        channels[ch]["revenue"] += float(o.get("AmountPaid") or 0)
        status = (o.get("StatusCode") or o.get("Status") or "").upper()
        if "CANCEL" in status:
            channels[ch]["cancelled"] += 1
        if str(o.get("CashOnDelivery") or "").lower() == "true":
            channels[ch]["cod_orders"] += 1

    total_orders = sum(v["orders"] for v in channels.values())
    total_revenue = sum(v["revenue"] for v in channels.values())

    performance = []
    for ch, v in sorted(channels.items(), key=lambda x: -x[1]["revenue"]):
        orders = v["orders"]
        rev = v["revenue"]
        performance.append({
            "channel": ch,
            "orders": orders,
            "revenue_inr": round(rev, 2),
            "avg_order_value_inr": round(rev / orders, 2) if orders else 0,
            "cancellation_rate_pct": round(v["cancelled"] / orders * 100, 1) if orders else 0,
            "cod_orders": v["cod_orders"],
            "cod_pct": round(v["cod_orders"] / orders * 100, 1) if orders else 0,
            "order_share_pct": round(orders / total_orders * 100, 1) if total_orders else 0,
            "revenue_share_pct": round(rev / total_revenue * 100, 1) if total_revenue else 0,
        })

    return {
        "period_days": days,
        "total_orders": total_orders,
        "total_revenue_inr": round(total_revenue, 2),
        "channels": performance,
    }


@mcp.tool()
async def get_fulfillment_health(days: int = 7) -> dict:
    """
    Fulfillment health dashboard: pending/processing orders, delayed shipments,
    backordered SKUs, and overall fulfillment rate. Use for daily ops check-ins.

    Args:
        days: look-back window for orders and shipments (default 7)
    """
    from datetime import timezone, timedelta
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=days)

    client = _client()
    try:
        # Fetch orders, shipments, and backorders in parallel
        order_req = build_search_sale_order_request(start=0, length=200, from_date=from_dt, to_date=to_dt)
        shipment_req = build_search_shipping_package_request(start=0, length=200, created_from=from_dt, created_to=to_dt)
        backorder_req = build_get_back_order_items_request()

        order_r, ship_r, bo_r = await asyncio.gather(
            client.call("SearchSaleOrder", order_req),
            client.call("SearchShippingPackage", shipment_req),
            client.call("GetBackOrderItems", backorder_req),
        )
    finally:
        await client.close()

    # Orders analysis
    order_p = order_r.payload if isinstance(order_r.payload, dict) else {}
    raw_orders = order_p.get("SaleOrders", {}).get("SaleOrder") if isinstance(order_p.get("SaleOrders"), dict) else []
    if not isinstance(raw_orders, list):
        raw_orders = [raw_orders] if raw_orders else []

    status_counts: dict[str, int] = {}
    for o in raw_orders:
        s = (o.get("StatusCode") or o.get("Status") or "UNKNOWN").upper()
        status_counts[s] = status_counts.get(s, 0) + 1

    pending_statuses = {"CREATED", "PROCESSING", "PENDING", "UNFULFILLED", "ON_HOLD"}
    pending_orders = sum(v for k, v in status_counts.items() if any(p in k for p in pending_statuses))
    cancelled_orders = sum(v for k, v in status_counts.items() if "CANCEL" in k)
    delivered_orders = sum(v for k, v in status_counts.items() if "DELIVER" in k or "COMPLETE" in k)
    total_orders = len(raw_orders)
    fulfillment_rate = round(delivered_orders / total_orders * 100, 1) if total_orders else None

    # Shipments analysis
    shipments = normalize_shipment_search_response(
        ship_r.payload if isinstance(ship_r.payload, dict) else {},
        settings.shipment_delay_threshold_hours,
    )
    delayed = [s for s in shipments if (s.get("delivery_delay_hours") or 0) > 0]
    in_transit = [s for s in shipments if (s.get("status") or "").upper() in {"DISPATCHED", "IN_TRANSIT", "OUT_FOR_DELIVERY"}]

    # Backorders
    backorders = normalize_back_order_items(bo_r.payload if isinstance(bo_r.payload, dict) else {})

    return {
        "period_days": days,
        "orders": {
            "total": total_orders,
            "pending": pending_orders,
            "delivered_or_complete": delivered_orders,
            "cancelled": cancelled_orders,
            "fulfillment_rate_pct": fulfillment_rate,
            "by_status": dict(sorted(status_counts.items(), key=lambda x: -x[1])),
        },
        "shipments": {
            "total_in_window": len(shipments),
            "in_transit": len(in_transit),
            "delayed_count": len(delayed),
            "delayed_shipments": [
                {
                    "shipment_code": s.get("shipment_code"),
                    "order_code": s.get("sale_order_code"),
                    "status": s.get("status"),
                    "delay_hours": s.get("delivery_delay_hours"),
                    "tracking": s.get("tracking_number"),
                    "provider": s.get("shipping_provider"),
                }
                for s in sorted(delayed, key=lambda x: -(x.get("delivery_delay_hours") or 0))[:20]
            ],
        },
        "backorders": {
            "total_sku_count": len(backorders),
            "skus": [
                {"sku_code": b.get("sku_code"), "product_name": b.get("product_name"), "quantity": b.get("quantity")}
                for b in backorders[:20]
            ],
        },
    }


@mcp.tool()
async def get_top_selling_skus(
    days: int = 30,
    top_n: int = 20,
    channel: str | None = None,
) -> dict:
    """
    Top-selling products by quantity sold and revenue. Fetches full order details
    in parallel to get item-level data. Use for product performance analysis.

    Args:
        days: look-back window (default 30)
        top_n: number of top SKUs to return (default 20)
        channel: optional channel filter (e.g. "AMAZON_IN")
    """
    from datetime import timezone, timedelta
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=days)

    client = _client()
    try:
        # Step 1: get order codes for the period
        order_codes: list[str] = []
        page_size = settings.page_size
        start = 0
        pages = 0
        while pages < settings.max_pages and len(order_codes) < 200:
            r = await client.call(
                "SearchSaleOrder",
                build_search_sale_order_request(start=start, length=page_size, from_date=from_dt, to_date=to_dt),
            )
            p = r.payload if isinstance(r.payload, dict) else {}
            total = int(p.get("TotalRecords") or 0)
            raw = p.get("SaleOrders", {}).get("SaleOrder") if isinstance(p.get("SaleOrders"), dict) else []
            if not isinstance(raw, list):
                raw = [raw] if raw else []
            for o in raw:
                if channel and (o.get("Channel") or "").upper() != channel.upper():
                    continue
                code = o.get("Code")
                if code:
                    order_codes.append(code)
            start += page_size
            pages += 1
            if start >= total or total == 0:
                break

        # Step 2: fetch full order details in parallel (cap at 100 orders)
        order_details = await _fetch_order_details_parallel(client, order_codes[:100])
    finally:
        await client.close()

    # Step 3: aggregate by SKU
    sku_stats: dict[str, dict] = {}
    for od in order_details:
        for item in od.get("items") or []:
            sku = item.get("sku_code")
            if not sku:
                continue
            status = (item.get("status") or "").upper()
            if "CANCEL" in status:
                continue  # exclude cancelled items
            qty = item.get("quantity") or 1
            rev = float(item.get("total_price") or 0)
            if sku not in sku_stats:
                sku_stats[sku] = {"sku_code": sku, "product_name": item.get("channel_product_id"), "quantity_sold": 0, "revenue_inr": 0.0, "order_count": 0}
            sku_stats[sku]["quantity_sold"] += qty
            sku_stats[sku]["revenue_inr"] += rev
            sku_stats[sku]["order_count"] += 1

    top_by_qty = sorted(sku_stats.values(), key=lambda x: -x["quantity_sold"])[:top_n]
    top_by_rev = sorted(sku_stats.values(), key=lambda x: -x["revenue_inr"])[:top_n]

    for item in top_by_qty + top_by_rev:
        item["revenue_inr"] = round(item["revenue_inr"], 2)

    return {
        "period_days": days,
        "orders_analysed": len(order_details),
        "unique_skus_sold": len(sku_stats),
        "top_by_quantity": top_by_qty,
        "top_by_revenue": top_by_rev,
    }


@mcp.tool()
async def call_unicommerce_soap(operation: str, payload_xml: str) -> dict:
    """
    Call any Unicommerce SOAP operation directly. Use this for operations not covered
    by the other tools.

    Args:
        operation: SOAP operation name (e.g. "GetSaleOrder")
        payload_xml: inner XML body of the request (the Request element or its children)
    """
    client = _client()
    try:
        body_element = ET.fromstring(payload_xml)
        response = await client.call(operation, body_element)
        return {"operation": operation, "payload": response.payload}
    finally:
        await client.close()
