# Namhya Unicommerce MCP

FastAPI + MCP server that exposes Unicommerce SOAP data directly to Claude (or any MCP client) with no database in the loop.

## Architecture

```
Claude / MCP Client
       │
       ▼
  FastMCP server  (http://localhost:8000/mcp)
       │
       ▼
  UnicommerceSoapClient  (WS-Security auth, retries)
       │
       ▼
  Unicommerce SOAP API  (namhyafood.unicommerce.co.in)
```

Every tool call hits Unicommerce live. No cache, no sync worker, no PostgreSQL.

## MCP Tools

| Tool | SOAP Operation | What it returns |
|---|---|---|
| `search_orders` | `SearchSaleOrder` | Paginated order summaries (code, status, channel, amount, dates) |
| `get_order` | `GetSaleOrder` | Full order — customer, address, line items, returns/cancellations |
| `search_shipments` | `SearchShippingPackage` | Shipments with tracking and delay hours |
| `get_inventory` | `SearchItemTypes` | Per-SKU per-warehouse stock levels |
| `get_channel_details` | `GetChannelDetails` | Channel name, enabled status |
| `call_unicommerce_soap` | any | Raw passthrough for any SOAP operation |

## Setup

1. Copy `.env.example` to `.env` and fill in credentials:

```env
UNICOMMERCE_USERNAME=your_api_username
UNICOMMERCE_PASSWORD=your_api_password
UNICOMMERCE_BASE_URL=https://namhyafood.unicommerce.co.in/services/soap/?version=1.9
UNICOMMERCE_WSDL_URL=https://namhyafood.unicommerce.co.in/services/soap/uniware19.wsdl
```

2. Start the server:

```bash
docker compose up --build
# or locally:
pip install -e .
uvicorn app.main:app --reload
```

## Connect Claude

Add this MCP server in Claude's settings:

```json
{
  "mcpServers": {
    "namhya-unicommerce": {
      "type": "http",
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

Claude can then query live Unicommerce data directly — orders, inventory, shipments, returns.

## Tuning

| Env var | Default | Effect |
|---|---|---|
| `PAGE_SIZE` | 50 | Records per SOAP page request |
| `MAX_PAGES` | 10 | Max pages per tool call (guards against huge fetches) |
| `LOW_STOCK_THRESHOLD` | 10 | Units threshold for `low_stock_only` filter |
| `SHIPMENT_DELAY_THRESHOLD_HOURS` | 24 | Hours beyond which a shipment is counted as delayed |

## Folder structure

```
app/
  services/
    normalizers.py     — parse SOAP XML dicts into clean Python dicts
    request_builders.py — build SOAP request XML elements
  config.py
  logging.py
  main.py             — FastAPI app + MCP mount
  mcp_server.py       — all MCP tool definitions
  soap.py             — async SOAP client with WS-Security + retries
  xml_utils.py        — XML helpers
tests/
docker-compose.yml
Dockerfile
pyproject.toml
```
