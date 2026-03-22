# CIGMA CRM System (Streamlit)

A Streamlit-based CRM system for sales and store operations with role login, dashboard KPIs, stock in/out, sales, orders, customer profiles, search, history, item image upload, and PDF exports.

## Features

- Role login: `admin`, `sales`
- Dashboard KPIs:
  - Sold-out items
  - Low-stock level
  - Stock in / Stock out volume
  - Total sales value
  - Item sales frequency chart
  - Active sales reps (last 30 days)
  - Orders placed / open
- Goods/Items management with image upload
- Stock In and Stock Out transactions
- Sales workflow with cart + receipt PDF
- Orders workflow (including dress-to-be-made orders) + order PDF
- Customer profile management
- Global search
- Record history / activity logs
- Reports page with CSV export

## Default Users

- Admin: `admin` / `admin123`
- Sales: `sales` / `sales123`

Change these immediately in production.

## Run

```bash
cd sales-store-management-system
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

## Notes

- Database file is created at `data/store_crm.db` (override with `DB_PATH` or `DATA_DIR`).
- Item images are stored in `uploads/` (override with `UPLOAD_DIR`).
- PDF files are generated on demand and downloaded via Streamlit buttons.
- For portable receipt printers, use the generated receipt PDF or connect via a dedicated print service (ESC/POS bridge).
- This version uses SQLite from Python standard library, so it runs even if you only have MySQL installed locally.

## Multi-Business (Single DB)

The app supports multiple businesses in one database using `business_id` scoping.

Environment options:
- `DEFAULT_BUSINESS_NAME` (default: `CIGMA Main Store`)
- `MAX_BUSINESSES` (default: `5`)

## Postgres

To use Postgres, set `DATABASE_URL` (Render provides this when you attach a Postgres database).
If `DATABASE_URL` is not set, the app falls back to SQLite at `data/store_crm.db`.
This project uses the pure-Python `pg8000` driver for compatibility across Python versions.

## Render Deploy

If you use Render, `render.yaml` is included for one-click deployment and persistent storage.
