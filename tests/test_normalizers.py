from decimal import Decimal

from app.services.normalizers import normalize_inventory_search_response, normalize_order, normalize_shipment_search_response


def test_normalize_order_rolls_up_revenue_and_returns():
    payload = {
        "SaleOrder": {
            "Code": "SO1",
            "DisplayOrderCode": "1001",
            "Channel": "SHOPIFY",
            "Status": "COMPLETE",
            "NotificationEmail": "a@example.com",
            "NotificationMobile": "9999999999",
            "CreatedOn": "2026-01-01T10:00:00+00:00",
            "UpdatedOn": "2026-01-01T11:00:00+00:00",
            "Addresses": {"Address": [{"Name": "Test", "City": "Delhi", "State": "Delhi", "Country": "IN", "Pincode": "110001"}]},
            "SaleOrderItems": {
                "SaleOrderItem": [
                    {
                        "Code": "SOI1",
                        "ItemSKU": "SKU1",
                        "StatusCode": "DELIVERED",
                        "Quantity": "2",
                        "SellingPrice": "100",
                        "TotalPrice": "200",
                        "FacilityCode": "WH1",
                    },
                    {
                        "Code": "SOI2",
                        "ItemSKU": "SKU2",
                        "StatusCode": "CANCELLED",
                        "Quantity": "1",
                        "TotalPrice": "50",
                        "FacilityCode": "WH1",
                        "CancellationReason": "Customer changed mind",
                    },
                ]
            },
        }
    }

    normalized = normalize_order(payload)
    assert normalized["order"]["total_revenue"] == Decimal("250")
    assert len(normalized["items"]) == 2
    assert len(normalized["returns"]) == 1
    assert normalized["returns"][0]["record_type"] == "cancellation"


def test_normalize_inventory_search_response_expands_snapshots():
    payload = {
        "ItemTypes": {
            "ItemType": {
                "SKUCode": "SKU1",
                "Name": "Product",
                "CategoryName": "Supplements",
                "InventorySnapshots": {"InventorySnapshot": {"Facility": "WH1", "Inventory": "12", "OpenSale": "1", "OpenPurchase": "0", "PutawayPending": "0", "InventoryBlocked": "0"}},
            }
        }
    }
    normalized = normalize_inventory_search_response(payload)
    assert normalized["skus"][0]["sku_code"] == "SKU1"
    assert normalized["inventory_snapshots"][0]["inventory"] == 12


def test_normalize_shipment_search_response_computes_delay():
    payload = {
        "ShippingPackages": {
            "ShippingPackage": {
                "Code": "SP1",
                "SaleOrderCode": "SO1",
                "Status": "DELIVERED",
                "CreatedOn": "2026-01-01T00:00:00+00:00",
                "UpdatedOn": "2026-01-03T01:00:00+00:00",
                "TrackingNumber": "TRK1",
            }
        }
    }
    normalized = normalize_shipment_search_response(payload, delay_threshold_hours=24)
    assert normalized[0]["delivery_delay_hours"] == 25
