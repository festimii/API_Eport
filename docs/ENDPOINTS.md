# API Endpoint Guide

This document summarizes all available endpoints in the AlgoRetail Push Data API and how to call them. All endpoints are rooted at `/` and the OpenAPI/Swagger UI is disabled, so refer to this guide for usage details.

## Access and Authentication
- **IP allowlist**: Requests from IPs in `ALLOWED_IPS` or `ALLOWED_IP_NETWORKS` bypass token verification, otherwise a bearer token is required.
- **Bearer tokens**: Obtain a JWT via `POST /auth/token` and include it in requests using the `Authorization: Bearer <token>` header. Tokens are required for every endpoint except the root health check.

### Obtain a token
- **Endpoint**: `POST /auth/token`
- **Body** (`application/json`):
  ```json
  {
    "username": "<API_USERNAME>",
    "password": "<API_PASSWORD>"
  }
  ```
- **Response**: `{ "access_token": "...", "token_type": "bearer", "expires_in": <seconds> }`

## Health
- **Endpoint**: `GET /`
- **Description**: Returns service status, version, and current UTC timestamp. No authentication required.

## Items API (`/items`)
All items endpoints require bearer authentication unless your IP is allowlisted.

- **POST /items/sync**: Run the `Sync_ItemMaster` stored procedure. No body; returns `{ "status": "OK", "message": "ItemMaster synchronized" }` once the background thread completes.
- **GET /items/stream?since=<timestamp>**: Streams all items as a JSON array without loading them into memory. When `since` is omitted, it defaults to the start of the current day (`YYYY-MM-DD 00:00:00`).
- **GET /items**: Returns the full `ItemMaster` table as JSON.
- **GET /items/{internal_id}**: Returns a single item by `Internal_ID`, or `404` when not found.

## Export (`/export`)
- **GET /export/sync**: Runs the `Sync_ItemMaster` stored procedure and reports completion.
- **GET /export/json**: Exports `ItemMaster` rows as JSON using a stable column order.
- **GET /export/csv**: Downloads the same export as a CSV file (`text/csv`, filename `ItemMaster.csv`).

## Sales (`/sales`)
- **POST /sales/push**: Executes `Api_Push_Sales` to populate today’s sales outbox.
- **GET /sales**: Lists sales outbox entries with optional filters:
  - `status` (string): Filter by delivery/status flag when present. If omitted, delivered rows are excluded when a status column exists.
  - `since` (ISO 8601 string): Return records updated on/after this timestamp when a timestamp column exists.
  - `limit` (1–1000, default 100) and `offset` (default 0): Pagination controls.
  - `group_by_bill` (bool, default `false`): When `true`, group results by detected bill identifier and include counts per bill.
  - **Response shape**: `{ "status": "OK", "metadata": { ... }, "data": [...] }`, with `metadata` summarizing pagination, detected column names, and status counts.
- **GET /sales/grouped**: Convenience endpoint to always group by bill identifier with the same filters except `group_by_bill`.
- **POST /sales/bills/delivered**: Marks the provided bill identifiers as delivered in bulk.
  - **Body**:
    ```json
    { "bill_ids": ["<bill_id1>", "<bill_id2>"] }
    ```
  - Returns affected row counts and per-bill summaries.
- **POST /sales/{sale_uid}/failed**: Marks a sale as failed.
  - **Body**:
    ```json
    { "reason": "<optional failure note>" }
    ```

## Stock (`/stock`)
- **GET /stock/daily?date=YYYY-MM-DD**: Executes `Festim_Stock_Export` for the given date (defaults to today) and returns the export rows.

## Transfers
- **GET /income**: Returns pending income (`pranim`) invoice lines from the secondary database.
- **GET /returns**: Returns pending return (`kthim`) invoice lines from the secondary database.
