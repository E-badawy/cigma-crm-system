import hashlib
import io
import os
from typing import Any, Dict, Iterable, List, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import base64
from datetime import date, datetime, timedelta
from uuid import uuid4

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.getenv("DATA_DIR", os.path.join(BASE_DIR, "data"))
DB_PATH = os.getenv("DB_PATH", os.path.join(DATA_DIR, "store_crm.db"))
UPLOAD_DIR = os.getenv("UPLOAD_DIR", os.path.join(BASE_DIR, "uploads"))
USER_UPLOAD_DIR = os.path.join(UPLOAD_DIR, "users")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(USER_UPLOAD_DIR, exist_ok=True)

COMPANY_NAME = "CIGMA CRM System"
COMPANY_LEGAL = "Badawy Technologies"
COMPANY_EMAIL = "cigma.generalsolutions@gmial.com"
COMPANY_PHONE = "08065440075"
COMPANY_ADDRESS = "shop 9 and 11, tukur tukur, zaria kaduna."
COMPANY_CONTACT = f"{COMPANY_EMAIL} | {COMPANY_PHONE}"
APP_ENV = os.getenv("APP_ENV", "Local")
APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
DEFAULT_BUSINESS_NAME = os.getenv("DEFAULT_BUSINESS_NAME", "CIGMA Main Store")
MAX_BUSINESSES = int(os.getenv("MAX_BUSINESSES", "5"))
PAGE_ICONS = {
    "Home": "🏠",
    "Dashboard": "📊",
    "Sales": "💳",
    "Orders": "🧾",
    "Customers": "👥",
    "Reports": "📈",
    "Fashion Items": "🧵",
    "Stock In": "📦",
    "Activity Logs": "🕘",
    "User Management": "🛡️",
    "Records Explorer": "🔎",
    "Support Centre": "🛟",
}

_ENGINE = None


def _normalize_sql(sql: str, params: Any) -> Tuple[str, Dict[str, Any]]:
    if params is None:
        return sql, {}
    if isinstance(params, dict):
        return sql, params
    if isinstance(params, (list, tuple)):
        idx = 0
        out = []
        for ch in sql:
            if ch == "?":
                idx += 1
                out.append(f":p{idx}")
            else:
                out.append(ch)
        sql_out = "".join(out)
        param_map = {f"p{i + 1}": params[i] for i in range(idx)}
        return sql_out, param_map
    return sql, params


def get_engine():
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    db_url = os.getenv("DATABASE_URL", "").strip()
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    if db_url:
        _ENGINE = create_engine(db_url, pool_pre_ping=True, future=True)
    else:
        _ENGINE = create_engine(
            f"sqlite:///{DB_PATH}",
            connect_args={"check_same_thread": False},
            future=True,
        )
    return _ENGINE


def db_dialect():
    return get_engine().dialect.name


def _id_def():
    return "id SERIAL PRIMARY KEY" if db_dialect() == "postgresql" else "id INTEGER PRIMARY KEY AUTOINCREMENT"


def _get_columns(c, table: str) -> List[str]:
    if db_dialect() == "postgresql":
        rows = c.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name=:t
            """,
            {"t": table},
        ).fetchall()
        return [r["column_name"] for r in rows]
    rows = c.execute(f"PRAGMA table_info({table})").fetchall()
    return [r["name"] for r in rows]


class DBResult:
    def __init__(self, result, conn, is_insert: bool):
        self._result = result
        self._conn = conn
        self._is_insert = is_insert

    def fetchone(self):
        row = self._result.mappings().fetchone()
        return row

    def fetchall(self):
        return self._result.mappings().fetchall()

    @property
    def lastrowid(self):
        lr = getattr(self._result, "lastrowid", None)
        if lr:
            return lr
        if self._is_insert and db_dialect() == "postgresql":
            try:
                row = self._conn.execute(text("SELECT LASTVAL() AS id")).fetchone()
                return row[0] if row else None
            except Exception:
                return None
        return None


class DBConn:
    def __init__(self):
        self._conn = get_engine().connect()
        self._tx = self._conn.begin()

    def execute(self, sql: str, params: Any = None):
        sql_norm, params_norm = _normalize_sql(sql, params)
        is_insert = sql_norm.lstrip().lower().startswith("insert")
        result = self._conn.execute(text(sql_norm), params_norm)
        return DBResult(result, self._conn, is_insert)

    def executescript(self, script: str):
        for stmt in script.split(";"):
            if stmt.strip():
                self.execute(stmt)

    def commit(self):
        if self._tx is not None:
            self._tx.commit()
            self._tx = self._conn.begin()

    def rollback(self):
        if self._tx is not None:
            self._tx.rollback()
            self._tx = self._conn.begin()

    def close(self):
        if self._tx is not None:
            self._tx.commit()
            self._tx = None
        self._conn.close()


def h(p):
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def page_label(page_name):
    return f"{PAGE_ICONS.get(page_name, '📄')} {page_name}"


def has_manager_access(user=None):
    actor = user if user is not None else st.session_state.get("user", {})
    role = str((actor or {}).get("role", "")).strip().lower()
    return role in ("manager", "admin")


def conn():
    return DBConn()


def get_default_business_id(c):
    row = c.execute("SELECT id FROM businesses ORDER BY id ASC LIMIT 1").fetchone()
    if row:
        return int(row["id"])
    c.execute("INSERT INTO businesses(name) VALUES(?)", (DEFAULT_BUSINESS_NAME,))
    return int(c.execute("SELECT last_insert_rowid() id").fetchone()["id"])


def current_business_id():
    bid = st.session_state.get("business_id")
    if bid:
        return int(bid)
    user = st.session_state.get("user")
    if user and user.get("business_id"):
        bid = int(user["business_id"])
        st.session_state.business_id = bid
        return bid
    return None


def require_business_id():
    bid = current_business_id()
    if bid:
        return bid
    c = conn()
    try:
        bid = get_default_business_id(c)
        c.commit()
    finally:
        c.close()
    st.session_state.business_id = bid
    return bid


def init_db():
    c = conn()
    id_def = _id_def()
    c.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS businesses(
            {id_def},
            name TEXT UNIQUE NOT NULL,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS users(
            {id_def},
            username TEXT UNIQUE, password_hash TEXT, role TEXT,
            is_active INTEGER DEFAULT 1, last_login TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            business_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS customers(
            {id_def},
            full_name TEXT NOT NULL, phone TEXT, email TEXT, address TEXT, notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            business_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS items(
            {id_def},
            sku TEXT UNIQUE NOT NULL, name TEXT NOT NULL, category TEXT,
            unit_price REAL NOT NULL, cost_price REAL DEFAULT 0,
            stock_qty INTEGER DEFAULT 0, reorder_level INTEGER DEFAULT 5,
            image_path TEXT, is_active INTEGER DEFAULT 1, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            business_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS stock_movements(
            {id_def},
            item_id INTEGER NOT NULL, movement_type TEXT NOT NULL, qty INTEGER NOT NULL,
            reference TEXT, notes TEXT, created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            business_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS sales(
            {id_def},
            invoice_no TEXT UNIQUE NOT NULL, customer_id INTEGER, sales_rep_id INTEGER,
            total_value REAL NOT NULL, status TEXT DEFAULT 'PAID', created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            business_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS sale_items(
            {id_def},
            sale_id INTEGER NOT NULL, item_id INTEGER NOT NULL, qty INTEGER NOT NULL,
            unit_price REAL NOT NULL, line_total REAL NOT NULL,
            business_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS orders(
            {id_def},
            order_no TEXT UNIQUE NOT NULL, customer_id INTEGER, description TEXT NOT NULL,
            status TEXT DEFAULT 'PENDING', due_date TEXT, total_value REAL DEFAULT 0,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            business_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS order_items(
            {id_def},
            order_id INTEGER NOT NULL,
            item_id INTEGER,
            item_name TEXT NOT NULL,
            sku TEXT,
            qty INTEGER NOT NULL DEFAULT 1,
            unit_price REAL NOT NULL DEFAULT 0,
            line_total REAL NOT NULL DEFAULT 0,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            business_id INTEGER,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        );
        CREATE TABLE IF NOT EXISTS activity_logs(
            {id_def},
            user_id INTEGER, action TEXT NOT NULL, entity_type TEXT, entity_id INTEGER,
            details TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            business_id INTEGER
        );
        """
    )
    default_business_id = get_default_business_id(c)
    if c.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        c.execute(
            "INSERT INTO users(username,password_hash,role,business_id) VALUES(?,?,?,?)",
            ("manager", h("manager123"), "manager", default_business_id),
        )
        c.execute(
            "INSERT INTO users(username,password_hash,role,business_id) VALUES(?,?,?,?)",
            ("sales", h("sales123"), "sales", default_business_id),
        )
    c.execute("UPDATE users SET role='manager' WHERE lower(role)='admin'")

    manager_user = c.execute("SELECT id,password_hash FROM users WHERE username='manager'").fetchone()
    legacy_admin_user = c.execute("SELECT id,password_hash FROM users WHERE username='admin'").fetchone()
    if manager_user:
        c.execute("UPDATE users SET role='manager', is_active=1 WHERE id=?", (int(manager_user["id"]),))
    elif legacy_admin_user:
        try:
            new_hash = h("manager123") if str(legacy_admin_user["password_hash"]) == h("admin123") else legacy_admin_user["password_hash"]
            c.execute(
                "UPDATE users SET username='manager', role='manager', password_hash=?, is_active=1 WHERE id=?",
                (new_hash, int(legacy_admin_user["id"])),
            )
        except IntegrityError:
            c.execute(
                "INSERT INTO users(username,password_hash,role,is_active,business_id) VALUES(?,?,?,?,?)",
                ("manager", h("manager123"), "manager", 1, default_business_id),
            )
    else:
        c.execute(
            "INSERT INTO users(username,password_hash,role,is_active,business_id) VALUES(?,?,?,?,?)",
            ("manager", h("manager123"), "manager", 1, default_business_id),
        )

    if c.execute("SELECT COUNT(*) c FROM users WHERE username='sales'").fetchone()["c"] == 0:
        c.execute(
            "INSERT INTO users(username,password_hash,role,is_active,business_id) VALUES(?,?,?,?,?)",
            ("sales", h("sales123"), "sales", 1, default_business_id),
        )
    cols = _get_columns(c, "users")
    if "photo_path" not in cols:
        c.execute("ALTER TABLE users ADD COLUMN photo_path TEXT")
    if "business_id" not in cols:
        c.execute("ALTER TABLE users ADD COLUMN business_id INTEGER")
        c.execute("UPDATE users SET business_id=? WHERE business_id IS NULL", (default_business_id,))

    items_cols = _get_columns(c, "items")
    items_migrations = {
        "description": "TEXT",
        "color": "TEXT",
        "updated_at": "TEXT",
    }
    if "business_id" not in items_cols:
        c.execute("ALTER TABLE items ADD COLUMN business_id INTEGER")
        c.execute("UPDATE items SET business_id=? WHERE business_id IS NULL", (default_business_id,))
    for col, definition in items_migrations.items():
        if col not in items_cols:
            c.execute(f"ALTER TABLE items ADD COLUMN {col} {definition}")

    customers_cols = _get_columns(c, "customers")
    customers_migrations = {
        "hand_length": "REAL",
        "neck_width": "REAL",
        "trouser_length": "REAL",
        "cap_size": "REAL",
        "chest_width": "REAL",
        "dress_length": "REAL",
        "waist_length": "REAL",
        "shoulder_length": "REAL",
        "measurement_unit": "TEXT DEFAULT 'cm'",
        "measurements_updated_at": "TEXT",
    }
    if "business_id" not in customers_cols:
        c.execute("ALTER TABLE customers ADD COLUMN business_id INTEGER")
        c.execute("UPDATE customers SET business_id=? WHERE business_id IS NULL", (default_business_id,))
    for col, definition in customers_migrations.items():
        if col not in customers_cols:
            c.execute(f"ALTER TABLE customers ADD COLUMN {col} {definition}")

    sales_cols = _get_columns(c, "sales")
    sales_migrations = {
        "subtotal": "REAL DEFAULT 0",
        "discount_total": "REAL DEFAULT 0",
        "tax_total": "REAL DEFAULT 0",
        "grand_total": "REAL DEFAULT 0",
        "payment_method": "TEXT",
        "amount_paid": "REAL DEFAULT 0",
        "balance_due": "REAL DEFAULT 0",
        "payment_status": "TEXT",
        "notes": "TEXT",
    }
    if "business_id" not in sales_cols:
        c.execute("ALTER TABLE sales ADD COLUMN business_id INTEGER")
        c.execute("UPDATE sales SET business_id=? WHERE business_id IS NULL", (default_business_id,))
    for col, definition in sales_migrations.items():
        if col not in sales_cols:
            c.execute(f"ALTER TABLE sales ADD COLUMN {col} {definition}")

    sale_items_cols = _get_columns(c, "sale_items")
    sale_items_migrations = {
        "sku": "TEXT",
        "item_name": "TEXT",
        "line_subtotal": "REAL DEFAULT 0",
        "line_discount": "REAL DEFAULT 0",
        "line_tax": "REAL DEFAULT 0",
        "discount_pct": "REAL DEFAULT 0",
        "tax_pct": "REAL DEFAULT 0",
    }
    if "business_id" not in sale_items_cols:
        c.execute("ALTER TABLE sale_items ADD COLUMN business_id INTEGER")
        c.execute("UPDATE sale_items SET business_id=? WHERE business_id IS NULL", (default_business_id,))
    for col, definition in sale_items_migrations.items():
        if col not in sale_items_cols:
            c.execute(f"ALTER TABLE sale_items ADD COLUMN {col} {definition}")

    orders_cols = _get_columns(c, "orders")
    orders_migrations = {
        "order_type": "TEXT DEFAULT 'DRESS_TO_BE_MADE'",
        "priority": "TEXT DEFAULT 'MEDIUM'",
        "progress_pct": "INTEGER DEFAULT 0",
        "assigned_to": "INTEGER",
        "updated_at": "TEXT",
        "notes": "TEXT",
        "qty": "INTEGER DEFAULT 1",
        "unit_price": "REAL DEFAULT 0",
    }
    if "business_id" not in orders_cols:
        c.execute("ALTER TABLE orders ADD COLUMN business_id INTEGER")
        c.execute("UPDATE orders SET business_id=? WHERE business_id IS NULL", (default_business_id,))
    for col, definition in orders_migrations.items():
        if col not in orders_cols:
            c.execute(f"ALTER TABLE orders ADD COLUMN {col} {definition}")

    order_items_cols = _get_columns(c, "order_items")
    if "business_id" not in order_items_cols:
        c.execute("ALTER TABLE order_items ADD COLUMN business_id INTEGER")
        c.execute("UPDATE order_items SET business_id=? WHERE business_id IS NULL", (default_business_id,))

    stock_cols = _get_columns(c, "stock_movements")
    if "business_id" not in stock_cols:
        c.execute("ALTER TABLE stock_movements ADD COLUMN business_id INTEGER")
        c.execute("UPDATE stock_movements SET business_id=? WHERE business_id IS NULL", (default_business_id,))

    logs_cols = _get_columns(c, "activity_logs")
    if "business_id" not in logs_cols:
        c.execute("ALTER TABLE activity_logs ADD COLUMN business_id INTEGER")
        c.execute("UPDATE activity_logs SET business_id=? WHERE business_id IS NULL", (default_business_id,))
    c.commit()
    c.close()


def log(action, entity="", entity_id=None, details=""):
    u = st.session_state.get("user")
    uid = u["id"] if u else None
    bid = current_business_id()
    c = conn()
    c.execute(
        "INSERT INTO activity_logs(user_id,action,entity_type,entity_id,details,business_id) VALUES(?,?,?,?,?,?)",
        (uid, action, entity, entity_id, details, bid),
    )
    c.commit()
    c.close()


def df(sql, params=()):
    sql_norm, params_norm = _normalize_sql(sql, params)
    with get_engine().connect() as c:
        return pd.read_sql_query(text(sql_norm), c, params=params_norm)


def calc_line(qty, unit_price, discount_pct, tax_pct):
    line_subtotal = float(qty) * float(unit_price)
    line_discount = line_subtotal * (float(discount_pct) / 100.0)
    taxable = max(0.0, line_subtotal - line_discount)
    line_tax = taxable * (float(tax_pct) / 100.0)
    line_total = taxable + line_tax
    return {
        "line_subtotal": round(line_subtotal, 2),
        "line_discount": round(line_discount, 2),
        "line_tax": round(line_tax, 2),
        "line_total": round(line_total, 2),
    }


def calc_cart_totals(cart_lines):
    subtotal = round(sum(float(x["line_subtotal"]) for x in cart_lines), 2)
    discount_total = round(sum(float(x["line_discount"]) for x in cart_lines), 2)
    tax_total = round(sum(float(x["line_tax"]) for x in cart_lines), 2)
    grand_total = round(sum(float(x["line_total"]) for x in cart_lines), 2)
    return {
        "subtotal": subtotal,
        "discount_total": discount_total,
        "tax_total": tax_total,
        "grand_total": grand_total,
    }


def recalc_order_totals(order_id):
    bid = require_business_id()
    c = conn()
    agg = c.execute(
        """
        SELECT
            COALESCE(SUM(qty), 0) AS qty_sum,
            COALESCE(SUM(line_total), 0) AS total_sum
        FROM order_items
        WHERE order_id=? AND business_id=?
        """,
        (int(order_id), bid),
    ).fetchone()
    qty_sum = int(agg["qty_sum"]) if agg else 0
    total_sum = float(agg["total_sum"]) if agg else 0.0
    unit_price = (total_sum / qty_sum) if qty_sum > 0 else 0.0
    c.execute(
        """
        UPDATE orders
        SET qty=?, unit_price=?, total_value=?, updated_at=?
        WHERE id=? AND business_id=?
        """,
        (
            qty_sum if qty_sum > 0 else 1,
            round(unit_price, 2),
            round(total_sum, 2),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            int(order_id),
            bid,
        ),
    )
    c.commit()
    c.close()


def customer_dependency_counts(customer_id):
    bid = require_business_id()
    c = conn()
    counts = c.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM orders WHERE customer_id=? AND business_id=?) AS order_count,
            (SELECT COUNT(*) FROM sales WHERE customer_id=? AND business_id=?) AS sale_count
        """,
        (int(customer_id), bid, int(customer_id), bid),
    ).fetchone()
    c.close()
    return int(counts["order_count"]), int(counts["sale_count"])


def delete_customer_record(customer_id):
    order_count, sale_count = customer_dependency_counts(customer_id)
    if order_count > 0 or sale_count > 0:
        raise ValueError("Cannot delete customer with linked orders or sales records.")
    bid = require_business_id()
    c = conn()
    c.execute("DELETE FROM customers WHERE id=? AND business_id=?", (int(customer_id), bid))
    c.commit()
    c.close()


def delete_order_record(order_id):
    bid = require_business_id()
    c = conn()
    c.execute("DELETE FROM order_items WHERE order_id=? AND business_id=?", (int(order_id), bid))
    c.execute("DELETE FROM orders WHERE id=? AND business_id=?", (int(order_id), bid))
    c.commit()
    c.close()


def item_dependency_counts(item_id):
    bid = require_business_id()
    c = conn()
    counts = c.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM sale_items WHERE item_id=? AND business_id=?) AS sale_line_count,
            (SELECT COUNT(*) FROM order_items WHERE item_id=? AND business_id=?) AS order_line_count,
            (SELECT COUNT(*) FROM stock_movements WHERE item_id=? AND business_id=?) AS movement_count
        """,
        (int(item_id), bid, int(item_id), bid, int(item_id), bid),
    ).fetchone()
    c.close()
    return int(counts["sale_line_count"]), int(counts["order_line_count"]), int(counts["movement_count"])


def delete_or_archive_item(item_id):
    sale_lines, order_lines, movement_count = item_dependency_counts(item_id)
    bid = require_business_id()
    c = conn()
    row = c.execute("SELECT image_path FROM items WHERE id=? AND business_id=?", (int(item_id), bid)).fetchone()
    image_path = row["image_path"] if row else None
    if sale_lines == 0 and order_lines == 0 and movement_count == 0:
        c.execute("DELETE FROM items WHERE id=? AND business_id=?", (int(item_id), bid))
        c.commit()
        c.close()
        if image_path:
            abs_path = os.path.abspath(os.path.join(BASE_DIR, str(image_path).replace("/", os.sep)))
            uploads_abs = os.path.abspath(UPLOAD_DIR)
            if abs_path.startswith(uploads_abs) and os.path.exists(abs_path):
                try:
                    os.remove(abs_path)
                except OSError:
                    pass
        return "deleted"
    c.execute(
        "UPDATE items SET is_active=0, updated_at=? WHERE id=? AND business_id=?",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), int(item_id), bid),
    )
    c.commit()
    c.close()
    return "archived"


def print_pdf_to_default_printer(pdf_bytes, doc_name):
    if not pdf_bytes:
        return False, "Receipt content is empty."
    if os.name != "nt":
        return False, "Direct print is currently supported only on Windows hosts."
    try:
        print_dir = os.path.join(DATA_DIR, "print_jobs")
        os.makedirs(print_dir, exist_ok=True)
        safe_doc = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(doc_name))[:48] or "receipt"
        temp_path = os.path.join(print_dir, f"{safe_doc}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
        with open(temp_path, "wb") as f:
            f.write(pdf_bytes)
        os.startfile(temp_path, "print")
        return True, f"Print job sent. File: {temp_path}"
    except Exception as ex:
        return False, f"Print failed: {ex}"


def create_sale_pdf(invoice_no):
    bid = require_business_id()
    c = conn()
    s = c.execute(
        """
        SELECT s.*, COALESCE(cu.full_name,'Walk-in') customer, COALESCE(u.username,'N/A') rep
        FROM sales s LEFT JOIN customers cu ON cu.id=s.customer_id LEFT JOIN users u ON u.id=s.sales_rep_id
        WHERE s.invoice_no=? AND s.business_id=?
        """,
        (invoice_no, bid),
    ).fetchone()
    lines = c.execute(
        """
        SELECT
            COALESCE(si.item_name, i.name) item_name,
            COALESCE(si.sku, i.sku) sku,
            si.qty,
            si.unit_price,
            COALESCE(si.line_subtotal, 0) line_subtotal,
            COALESCE(si.line_discount, 0) line_discount,
            COALESCE(si.line_tax, 0) line_tax,
            si.line_total
        FROM sale_items si
        LEFT JOIN items i ON i.id=si.item_id
        JOIN sales s ON s.id=si.sale_id
        WHERE s.invoice_no=? AND s.business_id=? AND si.business_id=?
        """,
        (invoice_no, bid, bid),
    ).fetchall()
    c.close()
    if not s:
        return b""
    payment_method = s["payment_method"] if "payment_method" in s.keys() and s["payment_method"] else "N/A"
    payment_status = s["payment_status"] if "payment_status" in s.keys() and s["payment_status"] else s["status"]
    subtotal = float(s["subtotal"]) if "subtotal" in s.keys() and s["subtotal"] is not None else float(s["total_value"])
    discount_total = float(s["discount_total"]) if "discount_total" in s.keys() and s["discount_total"] is not None else 0.0
    tax_total = float(s["tax_total"]) if "tax_total" in s.keys() and s["tax_total"] is not None else 0.0
    grand_total = float(s["grand_total"]) if "grand_total" in s.keys() and s["grand_total"] is not None else float(s["total_value"])
    amount_paid = float(s["amount_paid"]) if "amount_paid" in s.keys() and s["amount_paid"] is not None else grand_total
    balance_due = float(s["balance_due"]) if "balance_due" in s.keys() and s["balance_due"] is not None else 0.0
    notes = s["notes"] if "notes" in s.keys() and s["notes"] else ""

    b = io.BytesIO()
    cv = canvas.Canvas(b, pagesize=A4)
    w, hgt = A4
    left = 36
    right = w - 36
    x_no = left + 4
    x_desc = left + 24
    x_sku = left + 220
    x_qty = left + 286
    x_unit = left + 328
    x_disc = left + 386
    x_tax = left + 442
    x_amt = right - 6

    def draw_header():
        cv.setFillColorRGB(0.15, 0.2, 0.3)
        cv.rect(left, hgt - 92, right - left, 56, fill=1, stroke=0)
        cv.setFillColorRGB(1, 1, 1)
        cv.setFont("Helvetica-Bold", 16)
        cv.drawString(left + 10, hgt - 58, COMPANY_NAME)
        cv.setFont("Helvetica", 9)
        cv.drawString(left + 10, hgt - 72, f"{COMPANY_LEGAL} | {COMPANY_ADDRESS}")
        cv.drawString(left + 10, hgt - 83, COMPANY_CONTACT)
        cv.setFont("Helvetica-Bold", 14)
        cv.drawRightString(right - 10, hgt - 58, "SALES INVOICE")
        cv.setFont("Helvetica", 9)
        cv.drawRightString(right - 10, hgt - 72, f"Invoice #: {s['invoice_no']}")
        cv.drawRightString(right - 10, hgt - 83, f"Date: {str(s['created_at'])[:19]}")
        cv.setFillColorRGB(0, 0, 0)

        y_info_top = hgt - 106
        cv.rect(left, y_info_top - 52, (right - left) / 2 - 4, 52, fill=0, stroke=1)
        cv.rect(left + (right - left) / 2 + 4, y_info_top - 52, (right - left) / 2 - 4, 52, fill=0, stroke=1)

        cv.setFont("Helvetica-Bold", 9)
        cv.drawString(left + 8, y_info_top - 14, "Bill To")
        cv.setFont("Helvetica", 9)
        cv.drawString(left + 8, y_info_top - 27, f"Customer: {str(s['customer'])[:38]}")
        cv.drawString(left + 8, y_info_top - 40, f"Sales Rep: {str(s['rep'])[:38]}")

        x2 = left + (right - left) / 2 + 12
        cv.setFont("Helvetica-Bold", 9)
        cv.drawString(x2, y_info_top - 14, "Payment Info")
        cv.setFont("Helvetica", 9)
        cv.drawString(x2, y_info_top - 27, f"Method: {payment_method}")
        cv.drawString(x2, y_info_top - 40, f"Status: {payment_status}")

        return y_info_top - 62

    def draw_table_header(y):
        cv.setFillColorRGB(0.94, 0.94, 0.94)
        cv.rect(left, y - 15, right - left, 15, fill=1, stroke=1)
        cv.setFillColorRGB(0, 0, 0)
        cv.setFont("Helvetica-Bold", 8.5)
        cv.drawString(x_no, y - 11, "#")
        cv.drawString(x_desc, y - 11, "Description")
        cv.drawString(x_sku, y - 11, "SKU")
        cv.drawRightString(x_qty + 18, y - 11, "Qty")
        cv.drawRightString(x_unit + 44, y - 11, "Unit")
        cv.drawRightString(x_disc + 36, y - 11, "Disc")
        cv.drawRightString(x_tax + 28, y - 11, "Tax")
        cv.drawRightString(x_amt, y - 11, "Amount")
        return y - 18

    y = draw_header()
    y = draw_table_header(y)

    cv.setFont("Helvetica", 8.5)
    line_no = 1
    for r in lines:
        if y < 140:
            cv.showPage()
            y = draw_header()
            y = draw_table_header(y)
            cv.setFont("Helvetica", 8.5)
        item_name = str(r["item_name"] or "")[:34]
        sku = str(r["sku"] or "-")[:12]
        cv.line(left, y - 2, right, y - 2)
        cv.drawString(x_no, y - 11, str(line_no))
        cv.drawString(x_desc, y - 11, item_name)
        cv.drawString(x_sku, y - 11, sku)
        cv.drawRightString(x_qty + 18, y - 11, f"{int(r['qty'])}")
        cv.drawRightString(x_unit + 44, y - 11, f"{float(r['unit_price']):.2f}")
        cv.drawRightString(x_disc + 36, y - 11, f"{float(r['line_discount']):.2f}")
        cv.drawRightString(x_tax + 28, y - 11, f"{float(r['line_tax']):.2f}")
        cv.drawRightString(x_amt, y - 11, f"{float(r['line_total']):.2f}")
        y -= 16
        line_no += 1

    if y < 180:
        cv.showPage()
        y = draw_header()
        y = draw_table_header(y)

    box_w = 190
    box_x = right - box_w
    box_h = 94
    cv.rect(box_x, y - box_h, box_w, box_h, fill=0, stroke=1)
    cv.setFont("Helvetica", 9)
    cv.drawString(box_x + 8, y - 16, "Subtotal")
    cv.drawRightString(right - 8, y - 16, f"{subtotal:.2f}")
    cv.drawString(box_x + 8, y - 30, "Discount")
    cv.drawRightString(right - 8, y - 30, f"-{discount_total:.2f}")
    cv.drawString(box_x + 8, y - 44, "Tax")
    cv.drawRightString(right - 8, y - 44, f"+{tax_total:.2f}")
    cv.setFont("Helvetica-Bold", 10)
    cv.drawString(box_x + 8, y - 60, "Grand Total")
    cv.drawRightString(right - 8, y - 60, f"{grand_total:.2f}")
    cv.setFont("Helvetica", 9)
    cv.drawString(box_x + 8, y - 76, "Amount Paid")
    cv.drawRightString(right - 8, y - 76, f"{amount_paid:.2f}")
    cv.drawString(box_x + 8, y - 90, "Balance Due")
    cv.drawRightString(right - 8, y - 90, f"{balance_due:.2f}")

    notes_y = y - box_h - 12
    if notes:
        cv.setFont("Helvetica-Bold", 9)
        cv.drawString(left, notes_y, "Notes:")
        cv.setFont("Helvetica", 8.5)
        note_text = str(notes)
        y_note = notes_y - 12
        for i in range(0, min(len(note_text), 300), 85):
            cv.drawString(left, y_note, note_text[i:i + 85])
            y_note -= 11
        notes_y = y_note - 2

    sig_y = max(72, notes_y - 24)
    cv.line(left, sig_y, left + 140, sig_y)
    cv.line(left + 185, sig_y, left + 325, sig_y)
    cv.rect(right - 130, sig_y - 28, 120, 36, fill=0, stroke=1)
    cv.setFont("Helvetica", 8.5)
    cv.drawString(left, sig_y - 12, "Prepared By (Sales Rep)")
    cv.drawString(left + 185, sig_y - 12, "Authorized Signature")
    cv.drawCentredString(right - 70, sig_y - 10, "COMPANY STAMP")

    cv.setFont("Helvetica", 7.8)
    cv.drawString(left, 32, f"This is a system-generated invoice from {COMPANY_NAME}.")
    cv.drawRightString(right, 32, f"{COMPANY_CONTACT}")

    cv.showPage()
    cv.save()
    b.seek(0)
    return b.getvalue()


def create_order_pdf(order_no):
    bid = require_business_id()
    c = conn()
    o = c.execute(
        """
        SELECT
            o.*,
            COALESCE(cu.full_name,'N/A') customer,
            COALESCE(cu.phone,'') customer_phone,
            COALESCE(u.username,'Unassigned') assigned_to_user,
            COALESCE(cr.username,'N/A') created_by_user
        FROM orders o
        LEFT JOIN customers cu ON cu.id=o.customer_id
        LEFT JOIN users u ON u.id=o.assigned_to
        LEFT JOIN users cr ON cr.id=o.created_by
        WHERE o.order_no=? AND o.business_id=?
        """,
        (order_no, bid),
    ).fetchone()
    if not o:
        c.close()
        return b""

    order_lines = c.execute(
        """
        SELECT id, item_name, sku, qty, unit_price, line_total, COALESCE(notes,'') notes
        FROM order_items
        WHERE order_id=? AND business_id=?
        ORDER BY id ASC
        """,
        (int(o["id"]), bid),
    ).fetchall()
    c.close()

    order_type = o["order_type"] if "order_type" in o.keys() and o["order_type"] else "DRESS_TO_BE_MADE"
    priority = o["priority"] if "priority" in o.keys() and o["priority"] else "MEDIUM"
    progress = int(o["progress_pct"]) if "progress_pct" in o.keys() and o["progress_pct"] is not None else 0
    qty = int(o["qty"]) if "qty" in o.keys() and o["qty"] else 1
    unit_price = float(o["unit_price"]) if "unit_price" in o.keys() and o["unit_price"] is not None else 0.0
    total_value = float(o["total_value"]) if o["total_value"] is not None else float(qty) * unit_price
    notes = o["notes"] if "notes" in o.keys() and o["notes"] else ""

    b = io.BytesIO()
    cv = canvas.Canvas(b, pagesize=A4)
    w, hgt = A4
    left = 36
    right = w - 36
    y = hgt - 40

    cv.setFillColorRGB(0.15, 0.2, 0.3)
    cv.rect(left, hgt - 92, right - left, 56, fill=1, stroke=0)
    cv.setFillColorRGB(1, 1, 1)
    cv.setFont("Helvetica-Bold", 16)
    cv.drawString(left + 10, hgt - 58, COMPANY_NAME)
    cv.setFont("Helvetica", 9)
    cv.drawString(left + 10, hgt - 72, f"{COMPANY_LEGAL} | {COMPANY_ADDRESS}")
    cv.drawString(left + 10, hgt - 83, COMPANY_CONTACT)
    cv.setFont("Helvetica-Bold", 14)
    cv.drawRightString(right - 10, hgt - 58, "ORDER WORKSHEET")
    cv.setFont("Helvetica", 9)
    cv.drawRightString(right - 10, hgt - 72, f"Order #: {o['order_no']}")
    cv.drawRightString(right - 10, hgt - 83, f"Date: {str(o['created_at'])[:19]}")
    cv.setFillColorRGB(0, 0, 0)

    y_top = hgt - 106
    cv.rect(left, y_top - 66, (right - left) / 2 - 4, 66, fill=0, stroke=1)
    cv.rect(left + (right - left) / 2 + 4, y_top - 66, (right - left) / 2 - 4, 66, fill=0, stroke=1)

    cv.setFont("Helvetica-Bold", 9)
    cv.drawString(left + 8, y_top - 14, "Customer")
    cv.setFont("Helvetica", 9)
    cv.drawString(left + 8, y_top - 27, f"Name: {str(o['customer'])[:38]}")
    cv.drawString(left + 8, y_top - 40, f"Phone: {str(o['customer_phone'])[:38]}")
    cv.drawString(left + 8, y_top - 53, f"Due Date: {str(o['due_date'] or 'N/A')}")

    x2 = left + (right - left) / 2 + 12
    cv.setFont("Helvetica-Bold", 9)
    cv.drawString(x2, y_top - 14, "Order Meta")
    cv.setFont("Helvetica", 9)
    cv.drawString(x2, y_top - 27, f"Type: {order_type}")
    cv.drawString(x2, y_top - 40, f"Priority: {priority} | Status: {o['status']}")
    cv.drawString(x2, y_top - 53, f"Assigned To: {o['assigned_to_user']}")

    y = y_top - 82
    cv.setFillColorRGB(0.94, 0.94, 0.94)
    cv.rect(left, y - 15, right - left, 15, fill=1, stroke=1)
    cv.setFillColorRGB(0, 0, 0)
    cv.setFont("Helvetica-Bold", 8.5)
    cv.drawString(left + 8, y - 11, "Description")
    cv.drawString(right - 220, y - 11, "SKU")
    cv.drawString(right - 170, y - 11, "Qty")
    cv.drawString(right - 125, y - 11, "Unit")
    cv.drawRightString(right - 8, y - 11, "Amount")
    y -= 18

    cv.setFont("Helvetica", 8.5)
    line_count = 0
    if order_lines:
        for li in order_lines:
            if y < 170:
                cv.showPage()
                y = hgt - 80
                cv.setFillColorRGB(0.94, 0.94, 0.94)
                cv.rect(left, y - 15, right - left, 15, fill=1, stroke=1)
                cv.setFillColorRGB(0, 0, 0)
                cv.setFont("Helvetica-Bold", 8.5)
                cv.drawString(left + 8, y - 11, "Description")
                cv.drawString(right - 220, y - 11, "SKU")
                cv.drawString(right - 170, y - 11, "Qty")
                cv.drawString(right - 125, y - 11, "Unit")
                cv.drawRightString(right - 8, y - 11, "Amount")
                y -= 18
                cv.setFont("Helvetica", 8.5)
            cv.rect(left, y - 16, right - left, 16, fill=0, stroke=1)
            cv.drawString(left + 8, y - 11, str(li["item_name"])[:44])
            cv.drawString(right - 220, y - 11, str(li["sku"] or "-")[:12])
            cv.drawString(right - 170, y - 11, str(int(li["qty"])))
            cv.drawString(right - 125, y - 11, f"{float(li['unit_price']):.2f}")
            cv.drawRightString(right - 8, y - 11, f"{float(li['line_total']):.2f}")
            y -= 16
            line_count += 1
    else:
        description = o["description"] or ""
        desc_lines = [description[i:i + 72] for i in range(0, len(description), 72)] or [""]
        row_h = max(16, 11 * len(desc_lines))
        if y - row_h < 170:
            cv.showPage()
            cv.setFont("Helvetica", 8.5)
            y = hgt - 80
        cv.rect(left, y - row_h, right - left, row_h, fill=0, stroke=1)
        text_y = y - 11
        for ln in desc_lines[:6]:
            cv.drawString(left + 8, text_y, ln)
            text_y -= 11
        cv.drawString(right - 220, y - 11, "-")
        cv.drawString(right - 170, y - 11, f"{qty}")
        cv.drawString(right - 125, y - 11, f"{unit_price:.2f}")
        cv.drawRightString(right - 8, y - 11, f"{total_value:.2f}")
        y -= row_h
        line_count = 1
    y -= 12

    box_w = 190
    box_x = right - box_w
    box_h = 62
    cv.rect(box_x, y - box_h, box_w, box_h, fill=0, stroke=1)
    cv.setFont("Helvetica", 9)
    cv.drawString(box_x + 8, y - 16, "Line Items")
    cv.drawRightString(right - 8, y - 16, f"{line_count}")
    cv.drawString(box_x + 8, y - 30, "Progress")
    cv.drawRightString(right - 8, y - 30, f"{progress}%")
    cv.drawString(box_x + 8, y - 44, "Order Value")
    cv.drawRightString(right - 8, y - 44, f"{total_value:.2f}")
    cv.setFont("Helvetica-Bold", 10)
    cv.drawString(box_x + 8, y - 58, "Current Status")
    cv.drawRightString(right - 8, y - 58, f"{o['status']}")
    y -= box_h + 12

    if notes:
        cv.setFont("Helvetica-Bold", 9)
        cv.drawString(left, y, "Internal Notes:")
        cv.setFont("Helvetica", 8.5)
        y -= 12
        for i in range(0, min(len(notes), 280), 84):
            cv.drawString(left, y, notes[i:i + 84])
            y -= 11

    sig_y = max(72, y - 26)
    cv.line(left, sig_y, left + 140, sig_y)
    cv.line(left + 185, sig_y, left + 325, sig_y)
    cv.rect(right - 130, sig_y - 28, 120, 36, fill=0, stroke=1)
    cv.setFont("Helvetica", 8.5)
    cv.drawString(left, sig_y - 12, f"Prepared By ({o['created_by_user']})")
    cv.drawString(left + 185, sig_y - 12, "Authorized Signature")
    cv.drawCentredString(right - 70, sig_y - 10, "COMPANY STAMP")
    cv.setFont("Helvetica", 7.8)
    cv.drawString(left, 32, f"Generated by {COMPANY_NAME}.")
    cv.drawRightString(right, 32, COMPANY_CONTACT)
    cv.showPage()
    cv.save()
    b.seek(0)
    return b.getvalue()


def inject_styles(authenticated):
    app_bg = "linear-gradient(120deg, #fff3d6 0%, #ffd8b5 45%, #b8e0ff 100%)" if not authenticated else "#f4f6f8"
    login_form_css = ""
    login_layout_css = ""
    if not authenticated:
        login_form_css = """
        div[data-testid="stForm"]{
            background: rgba(255, 255, 255, 0.9);
            border: 1px solid #f1c86c;
            border-radius: 14px;
            padding: 0.8rem 1rem;
            box-shadow: 0 8px 24px rgba(96, 64, 0, 0.12);
        }
        div[data-testid="stForm"] label p{
            color: #5a3c00 !important;
            font-weight: 700 !important;
        }
        div[data-testid="stForm"] div[data-baseweb="input"] > div{
            border: 1.6px solid #b78a33 !important;
            border-radius: 10px !important;
            background: #ffffff !important;
        }
        div[data-testid="stForm"] div[data-baseweb="input"] > div:focus-within{
            border-color: #1f6cb7 !important;
            box-shadow: 0 0 0 3px rgba(31, 108, 183, 0.2) !important;
        }
        div[data-testid="stForm"] [data-testid="stFormSubmitButton"] > button{
            width: 100%;
            min-height: 42px;
            border-radius: 10px;
            font-weight: 700;
        }
        .login-inline-error{
            margin-top: 0.55rem;
            border: 1px solid #ef9b9b;
            background: #fff1f1;
            color: #8e1f1f;
            border-radius: 10px;
            padding: 0.5rem 0.65rem;
            font-size: 0.85rem;
            line-height: 1.35;
        }
        .login-support{
            margin-top: 0.8rem;
            border: 1px solid #d8e6f5;
            background: rgba(255, 255, 255, 0.82);
            border-radius: 12px;
            padding: 0.68rem 0.82rem;
            font-size: 0.86rem;
            color: #2f4865;
            line-height: 1.4;
        }
        .login-support a{
            color: #145ea7;
            text-decoration: none;
            font-weight: 700;
        }
        .login-support a:hover{
            text-decoration: underline;
        }
        .login-meta{
            margin-top: 0.28rem;
            color: #607891;
            font-size: 0.76rem;
        }
        @media (max-width: 900px){
            .main .block-container{
                padding-left: 0.7rem;
                padding-right: 0.7rem;
            }
            .login-support{
                font-size: 0.84rem;
            }
        }
        """
        login_layout_css = """
        [data-testid="stMain"]{
            display: flex;
            flex-direction: column;
        }
        [data-testid="stMainBlockContainer"]{
            min-height: calc(100vh - 64px);
            display: flex;
            flex-direction: column;
            flex: 1 1 auto;
            padding-bottom: 0 !important;
            margin-bottom: 0 !important;
        }
        .app-footer{
            margin-top: auto;
            margin-bottom: 0 !important;
        }
        """
    st.markdown(
        f"""
        <style>
        .stApp {{
            background: {app_bg};
        }}
        .main .block-container {{
            padding-bottom: 0;
        }}
        [data-testid="stMainBlockContainer"] {{
            padding-bottom: 0 !important;
            margin-bottom: 0 !important;
        }}
        footer[data-testid="stFooter"] {{
            display: none !important;
        }}
        .login-banner {{
            background: linear-gradient(135deg, #ffe59a 0%, #ff9a9e 55%, #8ec5fc 100%);
            border: 1px solid rgba(112, 69, 0, 0.25);
            border-radius: 16px;
            padding: 1rem 1.2rem;
            box-shadow: 0 10px 24px rgba(92, 56, 0, 0.14);
            margin-bottom: 0.9rem;
        }}
        .login-banner h2 {{
            margin: 0;
            color: #3b2100;
            font-size: 1.5rem;
        }}
        .login-banner p {{
            margin: 0.35rem 0 0 0;
            color: #4c2f00;
            font-size: 0.95rem;
        }}
        .top-nav-current {{
            background: #fff8e8;
            border: 1px solid #d4b46c;
            border-radius: 10px;
            padding: 0.45rem 0.75rem;
            margin-top: 0.2rem;
            color: #5a3600;
            font-size: 0.95rem;
        }}
        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, #b8860b 0%, #d4a017 48%, #f1c75c 100%);
            border-right: 1px solid #8f6200;
        }}
        [data-testid="stSidebar"] .stButton > button {{
            width: 100%;
            background: #fff6dc;
            color: #4a2d00;
            border: 1px solid #a97900;
            border-radius: 7px;
            padding: 0.5rem 0.75rem;
            text-align: left;
            font-weight: 600;
            transition: all 0.18s ease;
        }}
        [data-testid="stSidebar"] .stButton > button:hover {{
            background: #ffd569;
            color: #2f1b00;
            border-color: #845f00;
            transform: translateY(-1px);
            box-shadow: 0 4px 10px rgba(74, 45, 0, 0.26);
        }}
        div[data-testid="stPopover"] > button {{
            background: #fff6dc !important;
            color: #4a2d00 !important;
            border: 1px solid #a97900 !important;
            border-radius: 10px !important;
            font-weight: 700 !important;
            box-shadow: 0 2px 8px rgba(74, 45, 0, 0.12);
        }}
        div[data-testid="stPopover"] > button:hover {{
            background: #ffd569 !important;
            border-color: #845f00 !important;
            color: #2f1b00 !important;
        }}
        .stTextInput label p,
        .stTextArea label p,
        .stSelectbox label p,
        .stNumberInput label p,
        .stDateInput label p {{
            color: #5a3c00 !important;
            font-weight: 700 !important;
        }}
        div[data-baseweb="input"] > div,
        div[data-baseweb="select"] > div,
        div[data-baseweb="textarea"] {{
            background: #ffffff !important;
            border: 1.5px solid #b78a33 !important;
            border-radius: 10px !important;
        }}
        div[data-baseweb="input"] > div:focus-within,
        div[data-baseweb="select"] > div:focus-within,
        div[data-baseweb="textarea"]:focus-within {{
            border-color: #8a5a00 !important;
            box-shadow: 0 0 0 2px rgba(180, 128, 0, 0.18) !important;
        }}
        div[data-baseweb="input"] input,
        div[data-baseweb="textarea"] textarea {{
            color: #1f2937 !important;
            font-weight: 600 !important;
        }}
        div[data-baseweb="input"] input::placeholder,
        div[data-baseweb="textarea"] textarea::placeholder {{
            color: #6b7280 !important;
            opacity: 1 !important;
        }}
        div[data-baseweb="select"] span {{
            color: #1f2937 !important;
            font-weight: 600 !important;
        }}
        .stTabs [data-baseweb="tab-list"] {{
            gap: 0.45rem;
            background: #f7e4b7;
            border: 1px solid #d8b562;
            border-radius: 12px;
            padding: 0.3rem;
        }}
        .stTabs [data-baseweb="tab"] {{
            min-height: 44px;
            padding: 0.45rem 1.1rem;
            border: 1px solid #cfab5c;
            border-radius: 10px;
            background: #fff8e1;
            color: #533600;
            font-weight: 800;
            letter-spacing: 0.02em;
            flex: 1 1 0;
            justify-content: center;
        }}
        .stTabs [data-baseweb="tab"]:hover {{
            background: #ffe0a3;
            border-color: #a97900;
            color: #432a00;
        }}
        .stTabs [aria-selected="true"] {{
            background: #b97d0f !important;
            border-color: #7c4f00 !important;
            color: #fff8ea !important;
            box-shadow: 0 3px 10px rgba(123, 80, 0, 0.26);
        }}
        .home-hero {{
            background: linear-gradient(100deg, #1f2937 0%, #334155 48%, #475569 100%);
            border: 1px solid #4b5563;
            border-radius: 16px;
            padding: 1rem 1.2rem;
            margin-bottom: 0.85rem;
            color: #f8fafc;
            box-shadow: 0 12px 24px rgba(15, 23, 42, 0.18);
        }}
        .home-hero h2 {{
            margin: 0 0 0.35rem 0;
            font-size: 1.35rem;
            color: #f8fafc;
        }}
        .home-hero p {{
            margin: 0;
            color: #dbe7ff;
            font-size: 0.92rem;
        }}
        .kpi-card {{
            background: #fff8e1;
            border: 1px solid #e2c87a;
            border-radius: 12px;
            padding: 0.7rem 0.8rem;
            box-shadow: 0 4px 12px rgba(120, 88, 0, 0.1);
        }}
        .kpi-title {{
            color: #4b5563;
            font-size: 0.78rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        .kpi-value {{
            color: #0f172a;
            font-size: 1.35rem;
            font-weight: 800;
            margin: 0.2rem 0;
        }}
        .kpi-note {{
            color: #6d5a2f;
            font-size: 0.82rem;
        }}
        .kpi-alert .kpi-value {{
            color: #a11a1a;
        }}
        .kpi-good .kpi-value {{
            color: #0f5f3f;
        }}
        .app-footer {{
            position: static;
            width: 100%;
            margin-top: 0.7rem;
            margin-bottom: 0;
            background: linear-gradient(165deg, #06233c 0%, #083154 48%, #0a3c65 100%);
            border-top: 1px solid rgba(149, 200, 238, 0.26);
            border-radius: 0;
            padding: 0;
            color: #cbd5e1;
            box-shadow: none;
        }}
        .app-footer-grid {{
            display: grid;
            grid-template-columns: 1.2fr 1fr 1fr;
            gap: 0.75rem;
            width: min(1400px, calc(100% - 2rem));
            margin: 0 auto;
            padding: 0.72rem 0 0.56rem;
            line-height: 1.35;
            font-size: 0.88rem;
        }}
        .app-footer-title {{
            color: #f8fafc;
            font-weight: 700;
            margin-bottom: 0.3rem;
        }}
        .app-footer a {{
            color: #93c5fd;
            text-decoration: none;
        }}
        .app-footer a:hover {{
            color: #dbeafe;
            text-decoration: underline;
        }}
        @media (max-width: 900px) {{
            .app-footer-grid {{
                grid-template-columns: 1fr;
                gap: 0.45rem;
            }}
            .main .block-container {{
                padding-bottom: 0;
            }}
        }}
        {login_layout_css}
        .user-chip {{
            background: #fff6dd;
            border: 1px solid #e1c16e;
            border-radius: 12px;
            padding: 0.8rem;
            box-shadow: 0 4px 12px rgba(120, 88, 0, 0.12);
        }}
        .user-chip h4 {{
            margin: 0.2rem 0 0.2rem 0;
            color: #5a3c00;
        }}
        .user-chip p {{
            margin: 0;
            color: #7b6530;
            font-size: 0.86rem;
        }}
        .catalog-card {{
            background: #fff9ea;
            border: 1px solid #e1c57b;
            border-radius: 14px;
            padding: 0.6rem 0.65rem 0.75rem 0.65rem;
            box-shadow: 0 6px 14px rgba(123, 90, 24, 0.14);
            margin-bottom: 0.5rem;
        }}
        .catalog-thumb {{
            width: 100%;
            height: 155px;
            object-fit: cover;
            border-radius: 10px;
            border: 1px solid #d8b562;
            background: #f9efd0;
            margin-bottom: 0.5rem;
        }}
        .catalog-title {{
            color: #513500;
            font-weight: 800;
            font-size: 1rem;
            margin: 0;
        }}
        .catalog-sub {{
            color: #7a6230;
            font-size: 0.82rem;
            margin: 0.1rem 0;
        }}
        .catalog-price {{
            color: #0f5f3f;
            font-weight: 800;
            font-size: 0.95rem;
            margin: 0.2rem 0;
        }}
        .catalog-badges {{
            display: flex;
            gap: 0.35rem;
            flex-wrap: wrap;
            margin: 0.3rem 0 0.15rem 0;
        }}
        .catalog-badge {{
            background: #f3dfab;
            color: #5f4305;
            border: 1px solid #d8b562;
            border-radius: 999px;
            padding: 0.1rem 0.45rem;
            font-size: 0.72rem;
            font-weight: 700;
        }}
        .catalog-desc {{
            color: #6a5730;
            font-size: 0.8rem;
            margin-top: 0.22rem;
            min-height: 2.4rem;
        }}
        .item-detail-panel {{
            background: #fff9ea;
            border: 1px solid #e1c57b;
            border-radius: 14px;
            padding: 0.75rem 0.9rem;
            box-shadow: 0 6px 14px rgba(123, 90, 24, 0.14);
        }}
        div[data-testid="stMetric"] {{
            background: #fff6df;
            border: 1px solid #e1c577;
            border-radius: 10px;
            padding: 0.4rem 0.55rem;
            box-shadow: 0 2px 10px rgba(120, 88, 0, 0.1);
        }}
        div[data-testid="stMetricLabel"] {{
            color: #6d5a2f;
        }}
        div[data-testid="stMetricValue"] {{
            color: #5a3c00;
        }}
        {login_form_css}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_top_navigation(pages):
    current = st.session_state.get("page", pages[0] if pages else "")
    if not pages:
        return
    current_index = pages.index(current) if current in pages else 0
    c1, c2, c3 = st.columns([1.05, 3.0, 1.5])
    with c1:
        with st.popover("🧭 Navigation"):
            st.caption("Jump to page")
            for p in pages:
                if st.button(page_label(p), key=f"top_nav_{p}", use_container_width=True):
                    st.session_state.page = p
                    st.rerun()
    with c2:
        st.markdown(f"<div class='top-nav-current'>Current Page: <b>{page_label(current)}</b></div>", unsafe_allow_html=True)
    with c3:
        p1, p2 = st.columns(2)
        with p1:
            if st.button("◀ Prev", use_container_width=True, disabled=current_index == 0, key=f"top_nav_prev_{current_index}"):
                st.session_state.page = pages[current_index - 1]
                st.rerun()
        with p2:
            if st.button("Next ▶", use_container_width=True, disabled=current_index >= len(pages) - 1, key=f"top_nav_next_{current_index}"):
                st.session_state.page = pages[current_index + 1]
                st.rerun()


def user_avatar_data_uri(username, role):
    clean = (username or "U").strip()
    parts = [x for x in clean.replace("_", " ").split() if x]
    initials = "".join([p[0].upper() for p in parts[:2]]) or clean[:2].upper()
    bg = "#2563eb" if str(role).lower() in ["manager", "admin"] else "#059669"
    svg = f"""
    <svg xmlns='http://www.w3.org/2000/svg' width='112' height='112' viewBox='0 0 112 112'>
      <defs>
        <linearGradient id='g' x1='0' y1='0' x2='1' y2='1'>
          <stop offset='0%' stop-color='{bg}'/>
          <stop offset='100%' stop-color='#0f172a'/>
        </linearGradient>
      </defs>
      <rect width='112' height='112' rx='56' fill='url(#g)'/>
      <text x='56' y='66' text-anchor='middle' fill='#f8fafc' font-size='34' font-family='Arial' font-weight='700'>{initials}</text>
    </svg>
    """
    return "data:image/svg+xml;base64," + base64.b64encode(svg.encode("utf-8")).decode("utf-8")


def user_photo_data_uri(user):
    photo = (user or {}).get("photo_path")
    if photo:
        abs_path = os.path.join(BASE_DIR, photo.replace("/", os.sep))
        if os.path.exists(abs_path) and os.path.isfile(abs_path):
            ext = os.path.splitext(abs_path)[1].lower()
            mime = "image/png"
            if ext in [".jpg", ".jpeg"]:
                mime = "image/jpeg"
            elif ext == ".webp":
                mime = "image/webp"
            with open(abs_path, "rb") as f:
                return f"data:{mime};base64," + base64.b64encode(f.read()).decode("utf-8")
    return user_avatar_data_uri((user or {}).get("username", "User"), (user or {}).get("role", "sales"))


def item_image_data_uri(image_path, item_name):
    if image_path:
        abs_path = os.path.join(BASE_DIR, str(image_path).replace("/", os.sep))
        if os.path.exists(abs_path) and os.path.isfile(abs_path):
            ext = os.path.splitext(abs_path)[1].lower()
            mime = "image/png"
            if ext in [".jpg", ".jpeg"]:
                mime = "image/jpeg"
            elif ext == ".webp":
                mime = "image/webp"
            with open(abs_path, "rb") as f:
                return f"data:{mime};base64," + base64.b64encode(f.read()).decode("utf-8")

    label = (item_name or "Item").strip()
    initial = (label[0].upper() if label else "I")
    svg = f"""
    <svg xmlns='http://www.w3.org/2000/svg' width='420' height='280' viewBox='0 0 420 280'>
      <defs>
        <linearGradient id='ig' x1='0' y1='0' x2='1' y2='1'>
          <stop offset='0%' stop-color='#f9e8b7'/>
          <stop offset='100%' stop-color='#e5c981'/>
        </linearGradient>
      </defs>
      <rect width='420' height='280' fill='url(#ig)'/>
      <rect x='14' y='14' width='392' height='252' fill='none' stroke='#8a6a1f' stroke-width='2'/>
      <text x='210' y='152' text-anchor='middle' fill='#6a4b00' font-size='84' font-family='Arial' font-weight='700'>{initial}</text>
      <text x='210' y='236' text-anchor='middle' fill='#6a4b00' font-size='20' font-family='Arial'>No Image</text>
    </svg>
    """
    return "data:image/svg+xml;base64," + base64.b64encode(svg.encode("utf-8")).decode("utf-8")


def save_user_photo(user_id, uploaded_file):
    ext = os.path.splitext(uploaded_file.name)[1].lower()
    if ext not in [".png", ".jpg", ".jpeg", ".webp"]:
        raise ValueError("Unsupported image type.")

    bid = require_business_id()
    c = conn()
    existing = c.execute("SELECT photo_path FROM users WHERE id=? AND business_id=?", (int(user_id), bid)).fetchone()
    filename = f"user_{user_id}_{uuid4().hex}{ext}"
    target = os.path.join(USER_UPLOAD_DIR, filename)
    with open(target, "wb") as f:
        f.write(uploaded_file.getbuffer())
    rel = os.path.relpath(target, BASE_DIR).replace("\\", "/")
    c.execute("UPDATE users SET photo_path=? WHERE id=? AND business_id=?", (rel, int(user_id), bid))
    c.commit()
    updated = c.execute("SELECT * FROM users WHERE id=? AND business_id=?", (int(user_id), bid)).fetchone()
    c.close()

    old_rel = existing["photo_path"] if existing else None
    if old_rel and old_rel != rel:
        old_abs = os.path.abspath(os.path.join(BASE_DIR, old_rel.replace("/", os.sep)))
        user_dir_abs = os.path.abspath(USER_UPLOAD_DIR)
        if old_abs.startswith(user_dir_abs) and os.path.exists(old_abs):
            try:
                os.remove(old_abs)
            except OSError:
                pass

    current = st.session_state.get("user")
    if current and int(current.get("id", 0)) == int(user_id):
        st.session_state.user = dict(updated)
    return rel


def clear_user_photo(user_id):
    bid = require_business_id()
    c = conn()
    existing = c.execute("SELECT photo_path FROM users WHERE id=? AND business_id=?", (int(user_id), bid)).fetchone()
    c.execute("UPDATE users SET photo_path=NULL WHERE id=? AND business_id=?", (int(user_id), bid))
    c.commit()
    updated = c.execute("SELECT * FROM users WHERE id=? AND business_id=?", (int(user_id), bid)).fetchone()
    c.close()

    old_rel = existing["photo_path"] if existing else None
    if old_rel:
        old_abs = os.path.abspath(os.path.join(BASE_DIR, old_rel.replace("/", os.sep)))
        user_dir_abs = os.path.abspath(USER_UPLOAD_DIR)
        if old_abs.startswith(user_dir_abs) and os.path.exists(old_abs):
            try:
                os.remove(old_abs)
            except OSError:
                pass

    current = st.session_state.get("user")
    if current and int(current.get("id", 0)) == int(user_id):
        st.session_state.user = dict(updated)


def page_user_management():
    user = st.session_state.get("user", {})
    if not has_manager_access(user):
        st.error("Access denied. Manager only.")
        return
    bid = require_business_id()

    st.header("🛡️ User Management")
    st.caption("Manager workspace: manage user accounts, access roles, businesses, and profile photos.")

    biz_df = df("SELECT id, name, is_active, created_at FROM businesses ORDER BY name")
    biz_labels = {int(r["id"]): str(r["name"]) for _, r in biz_df.iterrows()} if not biz_df.empty else {}
    current_business_name = biz_labels.get(int(bid), "Current Business")
    st.markdown(f"**Current Business:** {current_business_name}")

    users_df = df(
        """
        SELECT id, username, role, is_active, last_login, created_at, photo_path
        FROM users
        WHERE business_id=:biz_id
        ORDER BY CASE WHEN lower(role)='manager' THEN 0 ELSE 1 END, username ASC
        """,
        {"biz_id": bid},
    )
    tab_profiles, tab_add_user, tab_businesses = st.tabs(["User Profiles", "Add New User", "Businesses"])

    with tab_profiles:
        if users_df.empty:
            st.info("No users found for this business.")
        else:
            display_df = users_df.copy()
            display_df["photo"] = display_df["photo_path"].apply(lambda x: "Yes" if isinstance(x, str) and x.strip() else "No")
            st.dataframe(
                display_df[["id", "username", "role", "is_active", "last_login", "created_at", "photo"]],
                use_container_width=True,
                hide_index=True,
            )

            options = users_df["id"].tolist()
            labels = {int(r["id"]): f"{r['username']} ({r['role']})" for _, r in users_df.iterrows()}
            selected_user_id = st.selectbox("Select user", options, format_func=lambda x: labels[int(x)])
            selected = users_df[users_df["id"] == selected_user_id].iloc[0].to_dict()

            col_l, col_r = st.columns([1, 1.35])
            with col_l:
                avatar = user_photo_data_uri(selected)
                st.markdown(
                    f"""
                    <div class="user-chip">
                        <img src="{avatar}" width="112" style="display:block;margin:auto;border-radius:50%;" />
                        <h4 style="text-align:center;">{selected.get('username', 'User')}</h4>
                        <p style="text-align:center;">Role: {selected.get('role', '-')}</p>
                        <p style="text-align:center;">Active: {"Yes" if int(selected.get('is_active', 0)) == 1 else "No"}</p>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            with col_r:
                uploader_key = f"manager_user_photo_upload_{int(selected_user_id)}"
                upload = st.file_uploader("Upload/replace user photo", type=["png", "jpg", "jpeg", "webp"], key=uploader_key)
                b1, b2 = st.columns(2)
                with b1:
                    if st.button("Save User Photo", use_container_width=True):
                        if upload is None:
                            st.warning("Choose an image first.")
                        else:
                            try:
                                save_user_photo(int(selected_user_id), upload)
                                log("MANAGER_UPDATE_USER_PHOTO", "users", int(selected_user_id), f"Photo updated for {selected.get('username')}")
                                st.success("User photo updated.")
                                st.rerun()
                            except ValueError as ex:
                                st.error(str(ex))
                with b2:
                    if st.button("Remove Photo", use_container_width=True):
                        clear_user_photo(int(selected_user_id))
                        log("MANAGER_REMOVE_USER_PHOTO", "users", int(selected_user_id), f"Photo removed for {selected.get('username')}")
                        st.success("User photo removed.")
                        st.rerun()

                t1, t2 = st.columns(2)
                with t1:
                    if st.button("Activate User", use_container_width=True):
                        c = conn()
                        c.execute("UPDATE users SET is_active=1 WHERE id=? AND business_id=?", (int(selected_user_id), bid))
                        c.commit()
                        c.close()
                        log("MANAGER_ACTIVATE_USER", "users", int(selected_user_id), f"Activated {selected.get('username')}")
                        st.success("User activated.")
                        st.rerun()
                with t2:
                    if st.button("Deactivate User", use_container_width=True):
                        if int(selected_user_id) == int(user.get("id", 0)):
                            st.warning("You cannot deactivate your own account while logged in.")
                        else:
                            c = conn()
                            c.execute("UPDATE users SET is_active=0 WHERE id=? AND business_id=?", (int(selected_user_id), bid))
                            c.commit()
                            c.close()
                            log("MANAGER_DEACTIVATE_USER", "users", int(selected_user_id), f"Deactivated {selected.get('username')}")
                            st.success("User deactivated.")
                            st.rerun()

    with tab_add_user:
        st.markdown("#### Create User Account")
        with st.form("create_user_form"):
            nu1, nu2 = st.columns(2)
            with nu1:
                new_username = st.text_input("Username", key="new_user_username")
                new_role = st.selectbox("Role", ["sales", "manager"], key="new_user_role")
            with nu2:
                new_password = st.text_input("Password", type="password", key="new_user_password")
                new_active = st.checkbox("Active", value=True, key="new_user_active")
            biz_options = list(biz_labels.keys())
            if biz_options:
                biz_index = biz_options.index(int(bid)) if int(bid) in biz_options else 0
                selected_biz = st.selectbox(
                    "Business",
                    biz_options,
                    index=biz_index,
                    format_func=lambda x: biz_labels.get(int(x), f"Business {x}"),
                    key="new_user_business",
                )
            else:
                selected_biz = bid
            create_user = st.form_submit_button("Add User")
        if create_user:
            if not new_username.strip():
                st.error("Username is required.")
            elif len(new_password) < 6:
                st.error("Password must be at least 6 characters.")
            else:
                c = conn()
                try:
                    cur = c.execute(
                        "INSERT INTO users(username,password_hash,role,is_active,business_id) VALUES(?,?,?,?,?)",
                        (
                            new_username.strip(),
                            h(new_password),
                            str(new_role).strip().lower(),
                            1 if new_active else 0,
                            int(selected_biz),
                        ),
                    )
                    c.commit()
                    c.close()
                    log(
                        "MANAGER_CREATE_USER",
                        "users",
                        int(cur.lastrowid),
                        f"Created user {new_username.strip()} ({new_role}) in business {biz_labels.get(int(selected_biz), selected_biz)}",
                    )
                    st.success("User account created.")
                    st.rerun()
                except IntegrityError:
                    c.close()
                    st.error("Username already exists.")

    with tab_businesses:
        st.markdown("#### Businesses")
        if biz_df.empty:
            st.info("No businesses found.")
        else:
            st.dataframe(
                biz_df[["id", "name", "is_active", "created_at"]],
                use_container_width=True,
                hide_index=True,
            )

        st.markdown("#### Add Business")
        business_count = int(len(biz_df)) if not biz_df.empty else 0
        can_add = business_count < MAX_BUSINESSES
        if not can_add:
            st.warning(f"Business limit reached ({MAX_BUSINESSES}). Increase MAX_BUSINESSES to add more.")
        with st.form("create_business_form"):
            new_business_name = st.text_input("Business name", key="new_business_name")
            submit_business = st.form_submit_button("Create Business", use_container_width=True, disabled=not can_add)
        if submit_business and can_add:
            if not new_business_name.strip():
                st.warning("Business name is required.")
            else:
                c = conn()
                try:
                    c.execute("INSERT INTO businesses(name) VALUES(?)", (new_business_name.strip(),))
                    c.commit()
                    c.close()
                    log("CREATE_BUSINESS", "businesses", None, f"Created business {new_business_name.strip()}")
                    st.success(f"Business created: {new_business_name.strip()}")
                    st.rerun()
                except IntegrityError:
                    c.close()
                    st.warning("Business name already exists.")

def render_footer():
    st.markdown(
        f"""
        <div class="app-footer">
            <div class="app-footer-grid">
                <div>
                    <div class="app-footer-title">CIGMA CRM System</div>
                    <div>Enterprise sales and store operations platform for inventory, orders, customers, and analytics.</div>
                    <div style="margin-top:0.45rem;">&copy; 2026 Badawy Technologies. All rights reserved.</div>
                </div>
                <div>
                    <div class="app-footer-title">Contact</div>
                    <div>Email: <a href="mailto:{COMPANY_EMAIL}">{COMPANY_EMAIL}</a></div>
                    <div>Phone: <a href="tel:{COMPANY_PHONE}">{COMPANY_PHONE}</a></div>
                    <div>Office: {COMPANY_ADDRESS}</div>
                </div>
                <div>
                    <div class="app-footer-title">Quick Services</div>
                    <div><a href="mailto:{COMPANY_EMAIL}?subject=Help%20Centre%20Request">Help Centre</a></div>
                    <div><a href="mailto:{COMPANY_EMAIL}?subject=Implementation%20Support%20Request">Implementation Support</a></div>
                    <div><a href="mailto:{COMPANY_EMAIL}?subject=Training%20and%20Onboarding%20Request">Training & Onboarding</a></div>
                    <div><a href="mailto:{COMPANY_EMAIL}?subject=Compliance%20and%20Audit%20Support%20Request">Compliance & Audit Assistance</a></div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def page_support():
    st.header("🛟 Support Centre")
    st.caption("Operational support workspace for help requests, implementation planning, onboarding, and compliance guidance.")

    t1, t2, t3, t4 = st.tabs(["Help Centre", "Implementation Support", "Training & Onboarding", "Compliance & Audit"])

    with t1:
        st.markdown("#### Help Centre")
        st.write("For troubleshooting login, records, sales posting, printing, and workflow navigation issues.")
        st.info(f"Contact: {COMPANY_PHONE} | {COMPANY_EMAIL}")
        with st.form("support_help_form"):
            issue = st.text_area("Describe issue", key="help_issue")
            submit_help = st.form_submit_button("Submit Help Request")
        if submit_help:
            if not issue.strip():
                st.warning("Please describe the issue before submitting.")
            else:
                log("SUPPORT_HELP_REQUEST", "support", None, issue.strip())
                st.success("Help request logged. Support team will follow up.")

    with t2:
        st.markdown("#### Implementation Support")
        st.write("Use this for setup changes, printer setup, module activation, and workflow configuration.")
        st.markdown("- Printer setup and defaults\n- User role setup\n- Catalogue and stock workflow design\n- Dashboard/report tuning")
        with st.form("support_impl_form"):
            request = st.text_area("Implementation request", key="impl_issue")
            submit_impl = st.form_submit_button("Submit Implementation Request")
        if submit_impl:
            if not request.strip():
                st.warning("Please enter your implementation request.")
            else:
                log("SUPPORT_IMPLEMENTATION_REQUEST", "support", None, request.strip())
                st.success("Implementation request logged.")

    with t3:
        st.markdown("#### Training & Onboarding")
        st.write("Request onboarding sessions for sales attendants and managers.")
        st.markdown("- Sales flow training\n- Orders and customer profiles\n- Reports interpretation\n- Daily closing process")
        with st.form("support_training_form"):
            training = st.text_area("Training request details", key="training_issue")
            submit_training = st.form_submit_button("Request Training")
        if submit_training:
            if not training.strip():
                st.warning("Please enter training requirements.")
            else:
                log("SUPPORT_TRAINING_REQUEST", "support", None, training.strip())
                st.success("Training request logged.")

    with t4:
        st.markdown("#### Compliance & Audit Assistance")
        st.write("Use this section for receipt controls, transaction logs, and audit trail support.")
        st.markdown("- Transaction traceability checks\n- Activity log review\n- Reconciliation checklist\n- Internal audit prep support")
        with st.form("support_compliance_form"):
            compliance = st.text_area("Compliance support request", key="compliance_issue")
            submit_compliance = st.form_submit_button("Submit Compliance Request")
        if submit_compliance:
            if not compliance.strip():
                st.warning("Please provide your compliance request.")
            else:
                log("SUPPORT_COMPLIANCE_REQUEST", "support", None, compliance.strip())
                st.success("Compliance support request logged.")


def login_view():
    st.markdown(
        """
        <div class="login-banner">
            <h2>CIGMA CRM System</h2>
            <p>Track stock in, orders, sales value, sold-out levels, customer records, and printable receipts.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption("Conventional home + login with manager/sales roles")
    a, b = st.columns([1.2, 1])
    with a:
        st.write("Modules: dashboard, sales, orders, stock in, goods/items, customers, search, support centre, activity logs, PDF receipts.")
    with b:
        login_error = ""
        with st.form("login"):
            u = st.text_input("Username", placeholder="Enter username")
            show_password = st.checkbox("Show password", value=False, key="show_login_password")
            p = st.text_input("Password", type="default" if show_password else "password", placeholder="Enter password")
            ok = st.form_submit_button("Log in", use_container_width=True)
        if ok:
            c = conn()
            username_input = u.strip()
            r = c.execute("SELECT * FROM users WHERE username=? AND is_active=1", (username_input,)).fetchone()
            if (not r) and username_input.lower() == "manager":
                legacy = c.execute("SELECT * FROM users WHERE username='admin' AND is_active=1", ()).fetchone()
                if legacy and legacy["password_hash"] == h(p):
                    try:
                        c.execute("UPDATE users SET username='manager', role='manager' WHERE id=?", (int(legacy["id"]),))
                        c.commit()
                        r = c.execute("SELECT * FROM users WHERE id=?", (int(legacy["id"]),)).fetchone()
                    except IntegrityError:
                        r = legacy
            if r and r["password_hash"] == h(p):
                default_business_id = get_default_business_id(c)
                if "business_id" not in r.keys() or r["business_id"] is None:
                    c.execute("UPDATE users SET business_id=? WHERE id=?", (default_business_id, r["id"]))
                c.execute("UPDATE users SET last_login=CURRENT_TIMESTAMP WHERE id=?", (r["id"],))
                c.commit()
                r = c.execute("SELECT * FROM users WHERE id=?", (r["id"],)).fetchone()
                c.close()
                st.session_state.user = dict(r)
                st.session_state.business_id = int(r["business_id"]) if r and r["business_id"] else default_business_id
                st.session_state.auth = True
                log("LOGIN", "users", r["id"], "User login")
                st.rerun()
            c.close()
            login_error = "Invalid credentials. If this is a migrated database, try manager/manager123 or admin/admin123 once."
        if login_error:
            st.markdown(f'<div class="login-inline-error">{login_error}</div>', unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class="login-support">
            <strong>Need access?</strong>
            Contact <a href="mailto:{COMPANY_EMAIL}?subject=Sales%20CRM%20Access%20Request">{COMPANY_EMAIL}</a>
            or <a href="tel:{COMPANY_PHONE}">{COMPANY_PHONE}</a>.
            <div><a href="mailto:{COMPANY_EMAIL}?subject=Sales%20CRM%20Help%20Request">Open Help Request</a></div>
            <div class="login-meta">Environment: {APP_ENV} • Version: {APP_VERSION}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_card(title, value, note="", kind=""):
    kind_class = f" {kind}" if kind else ""
    st.markdown(
        f"""
        <div class="kpi-card{kind_class}">
            <div class="kpi-title">{title}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def auto_collapse_sidebar_on_mobile():
    components.html(
        """
        <script>
        (function() {
          try {
            const width = window.innerWidth || 1024;
            if (width > 860) return;
            const doc = window.parent.document;
            const btn = doc.querySelector('button[data-testid="stSidebarCollapseButton"]');
            if (btn) { btn.click(); }
          } catch (e) {}
        })();
        </script>
        """,
        height=0,
        width=0,
    )


def page_home(user, pages):
    bid = require_business_id()
    today = date.today()
    today_start = f"{today.isoformat()} 00:00:00"
    tomorrow_start = f"{(today + timedelta(days=1)).isoformat()} 00:00:00"
    trend_start = f"{(today - timedelta(days=14)).isoformat()} 00:00:00"
    today_str = today.isoformat()
    today_label = datetime.now().strftime("%A, %B %d, %Y")
    st.markdown(
        f"""
        <div class="home-hero">
            <h2>🏠 Executive Home</h2>
            <p>Welcome back, <b>{user['username']}</b> ({user['role']}). Here is your live operations snapshot for {today_label}.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    snap = df(
        """
        SELECT
            (SELECT COUNT(*) FROM items WHERE is_active=1 AND business_id=:biz_id) items,
            (SELECT COUNT(*) FROM customers WHERE business_id=:biz_id) customers,
            (SELECT COUNT(*) FROM sales WHERE business_id=:biz_id) sales,
            (SELECT COUNT(*) FROM orders WHERE business_id=:biz_id) orders,
            (SELECT COALESCE(SUM(total_value),0) FROM sales WHERE business_id=:biz_id AND created_at>=:today_start AND created_at<:tomorrow_start) today_sales,
            (SELECT COUNT(*) FROM orders WHERE business_id=:biz_id AND created_at>=:today_start AND created_at<:tomorrow_start) today_orders,
            (SELECT COUNT(*) FROM items WHERE business_id=:biz_id AND is_active=1 AND stock_qty<=reorder_level) low_stock,
            (SELECT COUNT(*) FROM items WHERE business_id=:biz_id AND is_active=1 AND stock_qty<=0) sold_out,
            (SELECT COUNT(*) FROM orders WHERE business_id=:biz_id AND status IN ('PENDING','IN_PROGRESS')) open_orders,
            (SELECT COUNT(*) FROM orders WHERE business_id=:biz_id AND due_date IS NOT NULL AND due_date<:today_str AND status NOT IN ('COMPLETED','CANCELLED')) overdue_orders
        """,
        {"biz_id": bid, "today_start": today_start, "tomorrow_start": tomorrow_start, "today_str": today_str},
    ).iloc[0]

    row1 = st.columns(4)
    with row1[0]:
        render_kpi_card("Today's Sales", f"{float(snap['today_sales']):,.2f}", "Sales value posted today", "kpi-good")
    with row1[1]:
        render_kpi_card("Today's Orders", int(snap["today_orders"]), "Orders created today")
    with row1[2]:
        render_kpi_card("Open Orders", int(snap["open_orders"]), "Pending + In progress")
    with row1[3]:
        render_kpi_card("Overdue Orders", int(snap["overdue_orders"]), "Need immediate attention", "kpi-alert")

    row2 = st.columns(4)
    with row2[0]:
        render_kpi_card("Fashion Items", int(snap["items"]), "Active catalog items")
    with row2[1]:
        render_kpi_card("Customers", int(snap["customers"]), "Profiles in CRM")
    with row2[2]:
        render_kpi_card("Low Stock", int(snap["low_stock"]), "At or below reorder level", "kpi-alert")
    with row2[3]:
        render_kpi_card("Sold Out", int(snap["sold_out"]), "Zero stock items", "kpi-alert")

    st.markdown("#### Quick Actions")
    quick_pages = [p for p in ["Sales", "Orders", "Customers", "Reports", "Support Centre", "Fashion Items", "Stock In"] if p in pages]
    qcols = st.columns(len(quick_pages)) if quick_pages else []
    for idx, p in enumerate(quick_pages):
        with qcols[idx]:
            if st.button(f"Open {page_label(p)}", key=f"quick_{p}", use_container_width=True):
                st.session_state.page = p
                st.rerun()

    left, right = st.columns([1.4, 1])
    with left:
        st.markdown("#### Sales Trend (Last 14 Days)")
        trend = df(
            """
            SELECT DATE(created_at) day, COALESCE(SUM(total_value),0) value
            FROM sales
            WHERE business_id=:biz_id AND created_at>=:trend_start
            GROUP BY DATE(created_at)
            ORDER BY day
            """,
            {"biz_id": bid, "trend_start": trend_start},
        )
        if trend.empty:
            st.info("No sales trend available yet.")
        else:
            fig = px.area(trend, x="day", y="value", title="Daily Sales Value", markers=True)
            fig.update_layout(margin=dict(l=8, r=8, t=36, b=8))
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown("#### Operational Alerts")
        low_df = df(
            """
            SELECT sku, name, stock_qty, reorder_level
            FROM items
            WHERE business_id=:biz_id AND is_active=1 AND stock_qty<=reorder_level
            ORDER BY stock_qty ASC, name ASC
            LIMIT 8
            """,
            {"biz_id": bid},
        )
        overdue_df = df(
            """
            SELECT order_no, due_date, status, total_value
            FROM orders
            WHERE business_id=:biz_id AND due_date IS NOT NULL
              AND due_date<:today_str
              AND status NOT IN ('COMPLETED','CANCELLED')
            ORDER BY due_date ASC
            LIMIT 8
            """,
            {"biz_id": bid, "today_str": today_str},
        )
        st.caption("Low stock items")
        if low_df.empty:
            st.success("No low-stock alerts.")
        else:
            st.dataframe(low_df, use_container_width=True, hide_index=True)
        st.caption("Overdue orders")
        if overdue_df.empty:
            st.success("No overdue orders.")
        else:
            st.dataframe(overdue_df, use_container_width=True, hide_index=True)

    st.markdown("#### Recent Activity")
    logs = df(
        """
        SELECT l.created_at, COALESCE(u.username,'-') username, l.action, COALESCE(l.details,'') details
        FROM activity_logs l
        LEFT JOIN users u ON u.id=l.user_id
        WHERE l.business_id=:biz_id
        ORDER BY l.id DESC
        LIMIT 12
        """,
        {"biz_id": bid},
    )
    if logs.empty:
        st.info("No activity logs yet.")
    else:
        st.dataframe(logs, use_container_width=True, hide_index=True)


def page_dashboard():
    bid = require_business_id()
    active_since = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    st.header("📊 Dashboard")
    user = st.session_state.get("user", {"username": "User", "role": "sales"})
    c_left, c_right = st.columns([4, 1.3])
    with c_right:
        avatar = user_photo_data_uri(user)
        st.markdown(
            f"""
            <div class="user-chip">
                <img src="{avatar}" width="96" style="display:block;margin:auto;border-radius:50%;" />
                <h4 style="text-align:center;">{user.get('username', 'User')}</h4>
                <p style="text-align:center;">{user.get('role', 'sales').title()} Attendant</p>
                <p style="text-align:center;">Status: Active</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    k = df(
        """
        SELECT
            (SELECT COUNT(*) FROM items WHERE business_id=:biz_id AND stock_qty<=0 AND is_active=1) sold_out,
            (SELECT COUNT(*) FROM items WHERE business_id=:biz_id AND stock_qty<=reorder_level AND is_active=1) low_stock,
            (SELECT COALESCE(SUM(qty),0) FROM stock_movements WHERE business_id=:biz_id AND movement_type='IN') stock_in,
            (SELECT COUNT(*) FROM stock_movements WHERE business_id=:biz_id) inventory_moves,
            (SELECT COALESCE(SUM(total_value),0) FROM sales WHERE business_id=:biz_id) sales_value,
            (SELECT COUNT(*) FROM orders WHERE business_id=:biz_id) orders_count,
            (SELECT COUNT(*) FROM users WHERE business_id=:biz_id AND role='sales' AND last_login>=:active_since) active_sales
        """,
        {"biz_id": bid, "active_since": active_since},
    ).iloc[0]
    with c_left:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Sold Out", int(k["sold_out"]))
        c2.metric("Low Stock", int(k["low_stock"]))
        c3.metric("Stock In", int(k["stock_in"]))
        c4.metric("Inventory Moves", int(k["inventory_moves"]))
        c5, c6, c7 = st.columns(3)
        c5.metric("Sales Value", f"{float(k['sales_value']):,.2f}")
        c6.metric("Orders Placed", int(k["orders_count"]))
        c7.metric("Active Sales Reps", int(k["active_sales"]))
    left, right = st.columns(2)
    with left:
        f = df(
            """
            SELECT i.name item, COALESCE(SUM(si.qty),0) qty
            FROM items i
            LEFT JOIN sale_items si ON si.item_id=i.id AND si.business_id=:biz_id
            WHERE i.business_id=:biz_id
            GROUP BY i.id
            ORDER BY qty DESC
            LIMIT 12
            """,
            {"biz_id": bid},
        )
        st.plotly_chart(px.bar(f, x="item", y="qty", color="qty", title="Item Sales Frequency"), use_container_width=True)
    with right:
        t = df(
            "SELECT DATE(created_at) day, COALESCE(SUM(total_value),0) value FROM sales WHERE business_id=:biz_id GROUP BY DATE(created_at) ORDER BY day",
            {"biz_id": bid},
        )
        if t.empty:
            st.info("No sales yet.")
        else:
            st.plotly_chart(px.line(t, x="day", y="value", markers=True, title="Sales Trend"), use_container_width=True)


def page_items():
    bid = require_business_id()
    st.header("🧵 Fashion Items Catalogue")
    st.caption("Fashion catalogue with visual cards, detailed product profiles, and inventory registration.")
    is_manager = has_manager_access()

    if "item_detail_id" not in st.session_state:
        st.session_state.item_detail_id = None

    items_df = df(
        """
        SELECT
            id,
            sku,
            name,
            COALESCE(category, '') AS category,
            COALESCE(color, '') AS color,
            COALESCE(description, '') AS description,
            unit_price,
            cost_price,
            stock_qty,
            reorder_level,
            image_path,
            created_at,
            COALESCE(updated_at, created_at) AS updated_at
        FROM items
        WHERE business_id=:biz_id AND is_active=1
        ORDER BY id DESC
        """,
        {"biz_id": bid},
    )

    tab_catalog, tab_add, tab_inventory = st.tabs(["Catalogue View", "Add Item", "Inventory Register"])

    with tab_catalog:
        f1, f2, f3, f4 = st.columns([2.2, 1.4, 1.2, 1.2])
        with f1:
            q = st.text_input("Search catalogue", placeholder="Name, SKU, category, color...")
        with f2:
            categories = sorted([x for x in items_df["category"].dropna().unique().tolist() if str(x).strip()]) if not items_df.empty else []
            category_pick = st.selectbox("Category", ["All"] + categories)
        with f3:
            colors = sorted([x for x in items_df["color"].dropna().unique().tolist() if str(x).strip()]) if not items_df.empty else []
            color_pick = st.selectbox("Color", ["All"] + colors)
        with f4:
            sort_pick = st.selectbox("Sort By", ["Newest", "Price High-Low", "Price Low-High", "Name A-Z", "Low Stock First"])

        filtered = items_df.copy()
        if q.strip():
            needle = q.strip().lower()
            filtered = filtered[
                filtered["name"].str.lower().str.contains(needle, na=False)
                | filtered["sku"].str.lower().str.contains(needle, na=False)
                | filtered["category"].str.lower().str.contains(needle, na=False)
                | filtered["color"].str.lower().str.contains(needle, na=False)
                | filtered["description"].str.lower().str.contains(needle, na=False)
            ]
        if category_pick != "All":
            filtered = filtered[filtered["category"] == category_pick]
        if color_pick != "All":
            filtered = filtered[filtered["color"] == color_pick]

        if sort_pick == "Price High-Low":
            filtered = filtered.sort_values("unit_price", ascending=False)
        elif sort_pick == "Price Low-High":
            filtered = filtered.sort_values("unit_price", ascending=True)
        elif sort_pick == "Name A-Z":
            filtered = filtered.sort_values("name", ascending=True)
        elif sort_pick == "Low Stock First":
            filtered = filtered.sort_values("stock_qty", ascending=True)
        else:
            filtered = filtered.sort_values("id", ascending=False)

        if st.session_state.item_detail_id is not None:
            sel = filtered[filtered["id"] == int(st.session_state.item_detail_id)]
            if sel.empty:
                sel = items_df[items_df["id"] == int(st.session_state.item_detail_id)]
            if sel.empty:
                st.warning("Selected item was not found.")
                st.session_state.item_detail_id = None
                st.rerun()
            item = sel.iloc[0]

            if st.button("Back to Catalogue", key="item_back_catalogue"):
                st.session_state.item_detail_id = None
                st.rerun()

            d1, d2 = st.columns([1.2, 1.6])
            with d1:
                st.markdown("<div class='item-detail-panel'>", unsafe_allow_html=True)
                st.markdown(
                    f"<img src='{item_image_data_uri(item['image_path'], item['name'])}' style='width:100%;height:320px;object-fit:cover;border-radius:12px;border:1px solid #d8b562;'/>",
                    unsafe_allow_html=True,
                )
                st.markdown("</div>", unsafe_allow_html=True)
            with d2:
                st.markdown("<div class='item-detail-panel'>", unsafe_allow_html=True)
                st.markdown(f"### {item['name']}")
                st.write(f"**SKU:** {item['sku']}")
                st.write(f"**Category:** {item['category'] or '-'}")
                st.write(f"**Color:** {item['color'] or '-'}")
                st.write(f"**Sale Price:** {float(item['unit_price']):,.2f}")
                st.write(f"**Cost Price:** {float(item['cost_price']):,.2f}")
                st.write(f"**Quantity In Stock:** {int(item['stock_qty'])}")
                st.write(f"**Reorder Level:** {int(item['reorder_level'])}")
                st.write(f"**Date In:** {str(item['created_at'])[:19]}")
                st.write(f"**Last Update:** {str(item['updated_at'])[:19]}")
                st.write("**Description:**")
                st.write(item["description"] if str(item["description"]).strip() else "No description.")
                st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("#### Item Transaction Snapshot")
            tx1, tx2 = st.columns(2)
            with tx1:
                move_df = df(
                    """
                    SELECT movement_type, qty, reference, notes, created_at
                    FROM stock_movements
                    WHERE item_id=? AND business_id=?
                    ORDER BY id DESC
                    LIMIT 30
                    """,
                    (int(item["id"]), bid),
                )
                st.caption("Stock Movement History")
                if move_df.empty:
                    st.info("No stock movements yet.")
                else:
                    st.dataframe(move_df, use_container_width=True, hide_index=True)
            with tx2:
                sale_df = df(
                    """
                    SELECT
                        s.invoice_no,
                        si.qty,
                        si.unit_price,
                        si.line_total,
                        s.created_at
                    FROM sale_items si
                    JOIN sales s ON s.id=si.sale_id
                    WHERE si.item_id=? AND si.business_id=? AND s.business_id=?
                    ORDER BY si.id DESC
                    LIMIT 30
                    """,
                    (int(item["id"]), bid, bid),
                )
                st.caption("Sales Usage History")
                if sale_df.empty:
                    st.info("No sale lines for this item yet.")
                else:
                    st.dataframe(sale_df, use_container_width=True, hide_index=True)

        else:
            if filtered.empty:
                st.info("No items match these filters.")
            else:
                st.caption(f"{len(filtered)} item(s) in catalogue view")
                cards = filtered.to_dict("records")
                for i in range(0, len(cards), 3):
                    row_cols = st.columns(3)
                    for j in range(3):
                        idx = i + j
                        if idx >= len(cards):
                            continue
                        item = cards[idx]
                        with row_cols[j]:
                            created_date = str(item["created_at"])[:10]
                            desc = str(item["description"] or "")
                            desc_short = (desc[:94] + "...") if len(desc) > 95 else (desc if desc else "No description provided.")
                            stock_state = "Low Stock" if int(item["stock_qty"]) <= int(item["reorder_level"]) else "In Stock"
                            st.markdown(
                                f"""
                                <div class="catalog-card">
                                    <img src="{item_image_data_uri(item["image_path"], item["name"])}" class="catalog-thumb"/>
                                    <p class="catalog-title">{item["name"]}</p>
                                    <p class="catalog-sub">SKU: {item["sku"]}</p>
                                    <p class="catalog-sub">Category: {item["category"] or "-"}</p>
                                    <p class="catalog-sub">Color: {item["color"] or "-"}</p>
                                    <p class="catalog-price">Price: {float(item["unit_price"]):,.2f}</p>
                                    <div class="catalog-badges">
                                        <span class="catalog-badge">Qty: {int(item["stock_qty"])}</span>
                                        <span class="catalog-badge">Reorder: {int(item["reorder_level"])}</span>
                                        <span class="catalog-badge">{stock_state}</span>
                                        <span class="catalog-badge">Date In: {created_date}</span>
                                    </div>
                                    <div class="catalog-desc">{desc_short}</div>
                                </div>
                                """,
                                unsafe_allow_html=True,
                            )
                            if st.button("Open Full Product Info", key=f"item_open_{int(item['id'])}", use_container_width=True):
                                st.session_state.item_detail_id = int(item["id"])
                                st.rerun()

    with tab_add:
        st.subheader("Register New Fashion Item")
        with st.form("item_add"):
            c1, c2 = st.columns(2)
            with c1:
                sku = st.text_input("SKU")
                name = st.text_input("Name")
                cat = st.text_input("Category")
                color = st.text_input("Color")
                description = st.text_area("Description")
            with c2:
                sp = st.number_input("Sale Price", 0.0, step=0.5)
                cp = st.number_input("Cost Price", 0.0, step=0.5)
                qty = st.number_input("Opening Stock", 0, step=1)
                rl = st.number_input("Reorder Level", 0, value=5, step=1)
                img = st.file_uploader("Item image", type=["png", "jpg", "jpeg", "webp"])
            ok = st.form_submit_button("Save Item")

        if ok and sku.strip() and name.strip():
            ip = None
            if img:
                ext = os.path.splitext(img.name)[1] or ".jpg"
                fn = f"item_{uuid4().hex}{ext}"
                fp = os.path.join(UPLOAD_DIR, fn)
                with open(fp, "wb") as f:
                    f.write(img.getbuffer())
                ip = os.path.relpath(fp, BASE_DIR).replace("\\", "/")
            c = conn()
            try:
                cur = c.execute(
                    """
                    INSERT INTO items(
                        sku, name, category, color, description,
                        unit_price, cost_price, stock_qty, reorder_level, image_path, updated_at, business_id
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        sku.strip(),
                        name.strip(),
                        cat.strip(),
                        color.strip(),
                        description.strip(),
                        float(sp),
                        float(cp),
                        int(qty),
                        int(rl),
                        ip,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        bid,
                    ),
                )
                c.commit()
                log("CREATE_ITEM", "items", cur.lastrowid, f"{name.strip()} [{sku.strip()}]")
                st.success("Item saved.")
            except IntegrityError:
                st.error("SKU already exists.")
            c.close()

    with tab_inventory:
        st.subheader("Inventory Register")
        inv_df = df(
            """
            SELECT
                id, sku, name, category, color, unit_price, cost_price, stock_qty,
                reorder_level, created_at, updated_at, image_path, description
            FROM items
            WHERE business_id=:biz_id AND is_active=1
            ORDER BY id DESC
            """,
            {"biz_id": bid},
        )
        st.dataframe(inv_df, use_container_width=True, hide_index=True)
        if not inv_df.empty:
            st.download_button(
                "Download Inventory CSV",
                inv_df.to_csv(index=False).encode("utf-8"),
                file_name=f"inventory_{date.today().isoformat()}.csv",
                mime="text/csv",
            )
        if is_manager and not inv_df.empty:
            st.markdown("#### Item Maintenance")
            options = inv_df["id"].tolist()
            labels = {int(r["id"]): f"{r['name']} [{r['sku']}] (Qty: {int(r['stock_qty'])})" for _, r in inv_df.iterrows()}
            selected_item_id = st.selectbox("Select fashion item", options, format_func=lambda x: labels[int(x)], key="fashion_item_delete_pick")
            selected_item = inv_df[inv_df["id"] == selected_item_id].iloc[0]
            sale_lines, order_lines, movement_count = item_dependency_counts(int(selected_item_id))
            st.caption(
                f"Linked records -> Sales lines: {sale_lines}, Order lines: {order_lines}, Stock movements: {movement_count}"
            )
            st.caption("If linked records exist, the item will be archived (soft delete) instead of hard deleted.")
            confirm_item_delete = st.checkbox("Confirm item delete/archive", key="confirm_fashion_item_delete")
            if st.button("Delete / Archive Fashion Item", key="delete_fashion_item_btn", type="secondary"):
                if not confirm_item_delete:
                    st.warning("Confirm deletion first.")
                else:
                    mode = delete_or_archive_item(int(selected_item_id))
                    if mode == "deleted":
                        log("DELETE_ITEM", "items", int(selected_item_id), f"Deleted item {selected_item['name']}")
                        st.success("Item deleted permanently.")
                    else:
                        log("ARCHIVE_ITEM", "items", int(selected_item_id), f"Archived item {selected_item['name']}")
                        st.success("Item archived due to linked historical records.")
                    if st.session_state.get("item_detail_id") == int(selected_item_id):
                        st.session_state.item_detail_id = None
                    st.rerun()


def page_stock(move_type):
    bid = require_business_id()
    st.header("📦 Stock In" if move_type == "IN" else "📤 Stock Out")
    items_df = df(
        """
        SELECT id, sku, name, COALESCE(category,'') category, COALESCE(color,'') color,
               stock_qty, reorder_level, unit_price, COALESCE(updated_at, created_at) updated_at
        FROM items
        WHERE business_id=:biz_id AND is_active=1
        ORDER BY name
        """,
        {"biz_id": bid},
    )
    if items_df.empty:
        st.info("Add fashion items first.")
        return

    if move_type == "OUT":
        st.caption("Controlled stock release workflow with reason capture and projected inventory validation.")
        total_items = int(len(items_df))
        in_stock_count = int((items_df["stock_qty"] > 0).sum())
        low_stock_count = int((items_df["stock_qty"] <= items_df["reorder_level"]).sum())
        sold_out_count = int((items_df["stock_qty"] <= 0).sum())
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Active Items", total_items)
        m2.metric("In Stock", in_stock_count)
        m3.metric("Low Stock", low_stock_count)
        m4.metric("Sold Out", sold_out_count)

        available = items_df[items_df["stock_qty"] > 0].copy()
        if available.empty:
            st.warning("All items are sold out. No stock-out transaction can be posted.")
            return
        available = available.sort_values(["stock_qty", "name"], ascending=[True, True])

        labels = [
            f"{r['sku']} - {r['name']} | Stock: {int(r['stock_qty'])} | Reorder: {int(r['reorder_level'])}"
            for _, r in available.iterrows()
        ]
        idx = st.selectbox("Select item for stock out", range(len(labels)), format_func=lambda i: labels[i], key="stock_out_item_pick")
        row = available.iloc[idx]
        available_qty = int(row["stock_qty"])

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            qty = st.number_input("Quantity Out", min_value=1, max_value=available_qty, value=1, step=1, key="stock_out_qty")
        with c2:
            reason = st.selectbox("Reason", ["SALE", "ORDER_USAGE", "DAMAGE", "RETURN_TO_SUPPLIER", "LOSS", "ADJUSTMENT", "SAMPLE", "INTERNAL_USE"], key="stock_out_reason")
        with c3:
            ref = st.text_input("Reference", key="stock_out_reference")
        with c4:
            projected = int(available_qty) - int(qty)
            st.write("")
            st.metric("Projected Stock", projected)

        notes = st.text_area("Notes / Comment", key="stock_out_notes")
        if projected <= int(row["reorder_level"]):
            st.warning(f"Projected stock ({projected}) will be at or below reorder level ({int(row['reorder_level'])}).")

        if st.button("Post Stock Out", key="stock_out_post", type="primary"):
            if reason in ["DAMAGE", "LOSS", "ADJUSTMENT"] and not notes.strip():
                st.error("Notes are required for DAMAGE/LOSS/ADJUSTMENT stock out.")
            else:
                c = conn()
                c.execute(
                    "UPDATE items SET stock_qty=stock_qty-?, updated_at=? WHERE id=? AND business_id=?",
                    (int(qty), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), int(row["id"]), bid),
                )
                combined_notes = f"Reason={reason}; {notes.strip()}" if notes.strip() else f"Reason={reason}"
                cur = c.execute(
                    """
                    INSERT INTO stock_movements(item_id,movement_type,qty,reference,notes,created_by,business_id)
                    VALUES(?,?,?,?,?,?,?)
                    """,
                    (int(row["id"]), "OUT", int(qty), ref.strip(), combined_notes, st.session_state.user["id"], bid),
                )
                c.commit()
                c.close()
                log("STOCK_OUT", "stock_movements", cur.lastrowid, f"item={row['sku']} qty={int(qty)} reason={reason}")
                st.success("Stock out posted successfully.")
                st.rerun()

        st.markdown("#### Recent Stock-Out Transactions")
        out_df = df(
            """
            SELECT
                m.id,
                m.created_at,
                i.sku,
                i.name AS item_name,
                m.qty,
                m.reference,
                m.notes,
                COALESCE(u.username, '-') AS posted_by
            FROM stock_movements m
            JOIN items i ON i.id=m.item_id
            LEFT JOIN users u ON u.id=m.created_by
            WHERE m.business_id=:biz_id AND m.movement_type='OUT'
            ORDER BY m.id DESC
            LIMIT 120
            """,
            {"biz_id": bid},
        )
        st.dataframe(out_df, use_container_width=True, hide_index=True)
        if not out_df.empty:
            st.download_button(
                "Download Stock-Out CSV",
                out_df.to_csv(index=False).encode("utf-8"),
                file_name=f"stock_out_{date.today().isoformat()}.csv",
                mime="text/csv",
            )
        return

    labels = [f"{r.sku} - {r.name} (stock: {r.stock_qty})" for _, r in items_df.iterrows()]
    idx = st.selectbox("Item", range(len(labels)), format_func=lambda i: labels[i], key="stock_in_item_pick")
    row = items_df.iloc[idx]
    qty = st.number_input("Quantity In", 1, 1000000, 1, key="stock_in_qty")
    ref = st.text_input("Reference", key="stock_in_reference")
    notes = st.text_area("Notes", key="stock_in_notes")
    if st.button("Post Stock In", key="stock_in_post"):
        c = conn()
        c.execute(
            "UPDATE items SET stock_qty=stock_qty+?, updated_at=? WHERE id=? AND business_id=?",
            (int(qty), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), int(row["id"]), bid),
        )
        cur = c.execute(
            "INSERT INTO stock_movements(item_id,movement_type,qty,reference,notes,created_by,business_id) VALUES(?,?,?,?,?,?,?)",
            (int(row["id"]), "IN", int(qty), ref.strip(), notes.strip(), st.session_state.user["id"], bid),
        )
        c.commit()
        c.close()
        log("STOCK_IN", "stock_movements", cur.lastrowid, f"item={row['sku']} qty={qty}")
        st.success("Stock in recorded.")


def page_customers():
    bid = require_business_id()
    st.header("👥 Customer Profiles")
    is_manager = has_manager_access()
    measurement_fields = [
        ("hand_length", "Hand Length"),
        ("neck_width", "Neck Width"),
        ("trouser_length", "Trouser Length"),
        ("cap_size", "Cap Size"),
        ("chest_width", "Chest Width"),
        ("dress_length", "Dress Length"),
        ("waist_length", "Waist Length"),
        ("shoulder_length", "Shoulder Length"),
    ]

    with st.expander("Add Customer + Measurements", expanded=True):
        with st.form("customer_add"):
            c1, c2 = st.columns(2)
            with c1:
                n = st.text_input("Full name", key="cust_add_name")
                p = st.text_input("Phone", key="cust_add_phone")
                e = st.text_input("Email", key="cust_add_email")
                a = st.text_area("Address", key="cust_add_address")
            with c2:
                notes = st.text_area("Notes", key="cust_add_notes")
                unit = st.selectbox("Measurement Unit", ["cm", "inch"], key="cust_add_unit")

            st.markdown("**Measurements**")
            m1, m2, m3, m4 = st.columns(4)
            hand_length = m1.number_input("Hand Length", min_value=0.0, value=0.0, step=0.1, key="cust_add_hand")
            neck_width = m2.number_input("Neck Width", min_value=0.0, value=0.0, step=0.1, key="cust_add_neck")
            trouser_length = m3.number_input("Trouser Length", min_value=0.0, value=0.0, step=0.1, key="cust_add_trouser")
            cap_size = m4.number_input("Cap Size", min_value=0.0, value=0.0, step=0.1, key="cust_add_cap")
            m5, m6, m7, m8 = st.columns(4)
            chest_width = m5.number_input("Chest Width", min_value=0.0, value=0.0, step=0.1, key="cust_add_chest")
            dress_length = m6.number_input("Dress Length", min_value=0.0, value=0.0, step=0.1, key="cust_add_dress")
            waist_length = m7.number_input("Waist Length", min_value=0.0, value=0.0, step=0.1, key="cust_add_waist")
            shoulder_length = m8.number_input("Shoulder Length", min_value=0.0, value=0.0, step=0.1, key="cust_add_shoulder")
            ok = st.form_submit_button("Save Customer")

        if ok and n.strip():
            c = conn()
            cur = c.execute(
                """
                INSERT INTO customers(
                    full_name, phone, email, address, notes,
                    hand_length, neck_width, trouser_length, cap_size,
                    chest_width, dress_length, waist_length, shoulder_length,
                    measurement_unit, measurements_updated_at, business_id
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    n.strip(),
                    p.strip(),
                    e.strip(),
                    a.strip(),
                    notes.strip(),
                    float(hand_length),
                    float(neck_width),
                    float(trouser_length),
                    float(cap_size),
                    float(chest_width),
                    float(dress_length),
                    float(waist_length),
                    float(shoulder_length),
                    unit,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    bid,
                ),
            )
            c.commit()
            c.close()
            log("CREATE_CUSTOMER", "customers", cur.lastrowid, f"{n.strip()} with measurements")
            st.success("Customer saved with measurements.")

    q = st.text_input("Search customer", placeholder="Name / phone / email")
    base_sql = """
        SELECT
            c.*,
            (SELECT COUNT(*) FROM orders o WHERE o.customer_id=c.id AND o.business_id=:biz_id) AS order_count,
            (SELECT COUNT(*) FROM sales s WHERE s.customer_id=c.id AND s.business_id=:biz_id) AS sale_count
        FROM customers c
        WHERE c.business_id=:biz_id
    """
    if q.strip():
        customers_df = df(
            base_sql + " AND (c.full_name LIKE :q OR c.phone LIKE :q OR c.email LIKE :q) ORDER BY c.id DESC",
            {"biz_id": bid, "q": f"%{q}%"},
        )
    else:
        customers_df = df(base_sql + " ORDER BY c.id DESC", {"biz_id": bid})

    if customers_df.empty:
        st.info("No customers found.")
        return

    display_cols = [
        "id",
        "full_name",
        "phone",
        "email",
        "measurement_unit",
        "hand_length",
        "neck_width",
        "trouser_length",
        "cap_size",
        "chest_width",
        "dress_length",
        "waist_length",
        "shoulder_length",
        "order_count",
        "sale_count",
        "measurements_updated_at",
    ]
    st.dataframe(customers_df[display_cols], use_container_width=True, hide_index=True)

    st.markdown("#### Edit Customer Details + Measurements")
    options = customers_df["id"].tolist()
    labels = {int(r["id"]): f"{r['full_name']} (Orders: {r['order_count']}, Sales: {r['sale_count']})" for _, r in customers_df.iterrows()}
    selected_id = st.selectbox("Select customer profile", options, format_func=lambda x: labels[int(x)], key="customer_edit_pick")
    selected = customers_df[customers_df["id"] == selected_id].iloc[0]

    def _to_float(v):
        return float(v) if pd.notna(v) else 0.0

    with st.form("customer_edit_form"):
        e1, e2 = st.columns(2)
        with e1:
            en = st.text_input("Full name", value=str(selected["full_name"] or ""), key="cust_edit_name")
            ep = st.text_input("Phone", value=str(selected["phone"] or ""), key="cust_edit_phone")
            ee = st.text_input("Email", value=str(selected["email"] or ""), key="cust_edit_email")
            ea = st.text_area("Address", value=str(selected["address"] or ""), key="cust_edit_address")
        with e2:
            enotes = st.text_area("Notes", value=str(selected["notes"] or ""), key="cust_edit_notes")
            eunit = st.selectbox(
                "Measurement Unit",
                ["cm", "inch"],
                index=(["cm", "inch"].index(str(selected["measurement_unit"])) if str(selected["measurement_unit"]) in ["cm", "inch"] else 0),
                key="cust_edit_unit",
            )

        st.markdown("**Measurements**")
        em1, em2, em3, em4 = st.columns(4)
        e_hand = em1.number_input("Hand Length", min_value=0.0, value=_to_float(selected["hand_length"]), step=0.1, key="cust_edit_hand")
        e_neck = em2.number_input("Neck Width", min_value=0.0, value=_to_float(selected["neck_width"]), step=0.1, key="cust_edit_neck")
        e_trouser = em3.number_input("Trouser Length", min_value=0.0, value=_to_float(selected["trouser_length"]), step=0.1, key="cust_edit_trouser")
        e_cap = em4.number_input("Cap Size", min_value=0.0, value=_to_float(selected["cap_size"]), step=0.1, key="cust_edit_cap")
        em5, em6, em7, em8 = st.columns(4)
        e_chest = em5.number_input("Chest Width", min_value=0.0, value=_to_float(selected["chest_width"]), step=0.1, key="cust_edit_chest")
        e_dress = em6.number_input("Dress Length", min_value=0.0, value=_to_float(selected["dress_length"]), step=0.1, key="cust_edit_dress")
        e_waist = em7.number_input("Waist Length", min_value=0.0, value=_to_float(selected["waist_length"]), step=0.1, key="cust_edit_waist")
        e_shoulder = em8.number_input("Shoulder Length", min_value=0.0, value=_to_float(selected["shoulder_length"]), step=0.1, key="cust_edit_shoulder")
        save_edit = st.form_submit_button("Save Customer Changes")

    if save_edit:
        c = conn()
        c.execute(
            """
            UPDATE customers
            SET
                full_name=?,
                phone=?,
                email=?,
                address=?,
                notes=?,
                hand_length=?,
                neck_width=?,
                trouser_length=?,
                cap_size=?,
                chest_width=?,
                dress_length=?,
                waist_length=?,
                shoulder_length=?,
                measurement_unit=?,
                measurements_updated_at=?
            WHERE id=? AND business_id=?
            """,
            (
                en.strip(),
                ep.strip(),
                ee.strip(),
                ea.strip(),
                enotes.strip(),
                float(e_hand),
                float(e_neck),
                float(e_trouser),
                float(e_cap),
                float(e_chest),
                float(e_dress),
                float(e_waist),
                float(e_shoulder),
                eunit,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                int(selected_id),
                bid,
            ),
        )
        c.commit()
        c.close()
        log("UPDATE_CUSTOMER_MEASUREMENTS", "customers", int(selected_id), f"Updated measurements for {en.strip()}")
        st.success("Customer details and measurements updated.")
        st.rerun()

    if is_manager:
        st.markdown("#### Customer Maintenance")
        linked_orders = int(selected["order_count"])
        linked_sales = int(selected["sale_count"])
        st.caption(f"Linked records -> Orders: {linked_orders}, Sales: {linked_sales}")
        confirm_delete = st.checkbox("Confirm customer deletion", key="confirm_customer_delete")
        if st.button("Delete Customer", type="secondary", key="delete_customer_btn"):
            if not confirm_delete:
                st.warning("Confirm deletion first.")
            else:
                try:
                    delete_customer_record(int(selected_id))
                    log("DELETE_CUSTOMER", "customers", int(selected_id), f"Deleted customer {selected['full_name']}")
                    st.success("Customer deleted.")
                    st.rerun()
                except ValueError as ex:
                    st.error(str(ex))


def page_sales():
    bid = require_business_id()
    st.header("💳 Sales")
    if "sales_cart" not in st.session_state:
        st.session_state.sales_cart = []

    items = df(
        "SELECT id,sku,name,unit_price,stock_qty FROM items WHERE business_id=:biz_id AND is_active=1 ORDER BY name",
        {"biz_id": bid},
    )
    customers = df("SELECT id,full_name FROM customers WHERE business_id=:biz_id ORDER BY full_name", {"biz_id": bid})
    is_manager = has_manager_access()

    if items.empty:
        st.info("Add items first.")
        return

    with st.expander("Add Item To Cart", expanded=True):
        labels = [f"{r.sku} - {r.name} (stock: {r.stock_qty})" for _, r in items.iterrows()]
        idx = st.selectbox("Item", range(len(labels)), format_func=lambda x: labels[x], key="sale_item_pick")
        selected = items.iloc[idx]
        available_qty = int(selected["stock_qty"])

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            qty = st.number_input("Qty", min_value=1, max_value=max(1, available_qty), value=1, step=1, key="sale_qty_input")
        with c2:
            unit_price = st.number_input(
                "Unit Price",
                min_value=0.0,
                value=float(selected["unit_price"]),
                step=0.5,
                key="sale_unit_price_input",
                disabled=not is_manager,
            )
        with c3:
            discount_pct = st.number_input("Discount %", min_value=0.0, max_value=100.0, value=0.0, step=0.5, key="sale_discount_pct")
        with c4:
            tax_pct = st.number_input("Tax %", min_value=0.0, max_value=100.0, value=0.0, step=0.5, key="sale_tax_pct")

        if st.button("Add / Update Line", key="sale_add_line", use_container_width=True):
            if available_qty <= 0:
                st.error("Selected item is out of stock.")
            else:
                existing = next((x for x in st.session_state.sales_cart if int(x["item_id"]) == int(selected["id"])), None)
                new_qty = int(qty) + (int(existing["qty"]) if existing else 0)
                if new_qty > available_qty:
                    st.error(f"Only {available_qty} units are available.")
                else:
                    line_calc = calc_line(new_qty, float(unit_price), float(discount_pct), float(tax_pct))
                    line_data = {
                        "item_id": int(selected["id"]),
                        "sku": str(selected["sku"]),
                        "item_name": str(selected["name"]),
                        "qty": new_qty,
                        "unit_price": round(float(unit_price), 2),
                        "discount_pct": round(float(discount_pct), 2),
                        "tax_pct": round(float(tax_pct), 2),
                        **line_calc,
                    }
                    if existing:
                        existing.update(line_data)
                        st.success("Cart line updated.")
                    else:
                        st.session_state.sales_cart.append(line_data)
                        st.success("Cart line added.")

    st.subheader("Sales Cart")
    cart = st.session_state.sales_cart
    if cart:
        cart_df = pd.DataFrame(cart)[
            [
                "sku",
                "item_name",
                "qty",
                "unit_price",
                "discount_pct",
                "tax_pct",
                "line_subtotal",
                "line_discount",
                "line_tax",
                "line_total",
            ]
        ]
        st.dataframe(cart_df, use_container_width=True, hide_index=True)

        a1, a2 = st.columns([2, 1])
        with a1:
            remove_labels = [f"{x['sku']} - {x['item_name']}" for x in cart]
            remove_pick = st.selectbox("Remove line", range(len(remove_labels)), format_func=lambda i: remove_labels[i], key="sale_remove_pick")
        with a2:
            st.write("")
            if st.button("Remove Selected Line", key="sale_remove_line", use_container_width=True):
                del st.session_state.sales_cart[int(remove_pick)]
                st.rerun()
            if st.button("Clear Cart", key="sale_clear_cart", use_container_width=True):
                st.session_state.sales_cart = []
                st.rerun()
    else:
        st.info("Cart is empty. Add at least one line.")

    totals = calc_cart_totals(cart)
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Subtotal", f"{totals['subtotal']:.2f}")
    t2.metric("Discount Total", f"{totals['discount_total']:.2f}")
    t3.metric("Tax Total", f"{totals['tax_total']:.2f}")
    t4.metric("Grand Total", f"{totals['grand_total']:.2f}")

    st.markdown("#### Payment & Finalization")
    customer_names = ["Walk-in"] + customers["full_name"].tolist()
    p1, p2, p3 = st.columns(3)
    with p1:
        customer_name = st.selectbox("Customer", customer_names, key="sale_customer")
    with p2:
        payment_method = st.selectbox("Payment Method", ["CASH", "CARD", "BANK_TRANSFER", "MOBILE_WALLET", "CREDIT"], key="sale_payment_method")
    with p3:
        amount_paid = st.number_input(
            "Amount Paid",
            min_value=0.0,
            value=float(totals["grand_total"]),
            step=0.5,
            key="sale_amount_paid",
        )

    balance_due = round(max(0.0, float(totals["grand_total"]) - float(amount_paid)), 2)
    if totals["grand_total"] <= 0:
        payment_status = "UNPAID"
    elif amount_paid <= 0:
        payment_status = "UNPAID"
    elif amount_paid + 1e-9 < totals["grand_total"]:
        payment_status = "PARTIAL"
    else:
        payment_status = "PAID"
    st.caption(f"Payment Status: {payment_status} | Balance Due: {balance_due:.2f}")
    notes = st.text_area("Sale Notes", key="sale_notes")

    if st.button("Finalize Sale", type="primary", key="sale_finalize"):
        if not cart:
            st.error("Cart is empty.")
        else:
            conn_db = conn()
            try:
                for line in cart:
                    stock_row = conn_db.execute(
                        "SELECT stock_qty FROM items WHERE id=? AND business_id=?",
                        (int(line["item_id"]), bid),
                    ).fetchone()
                    if not stock_row or int(stock_row["stock_qty"]) < int(line["qty"]):
                        raise ValueError(f"Insufficient stock for {line['item_name']}.")

                invoice_no = f"INV-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:5].upper()}"
                customer_id = None
                if customer_name != "Walk-in":
                    customer_id = int(customers[customers["full_name"] == customer_name]["id"].iloc[0])

                legacy_status = "PAID" if payment_status == "PAID" else "PENDING"
                sale_cur = conn_db.execute(
                    """
                    INSERT INTO sales(
                        invoice_no, customer_id, sales_rep_id, total_value, status,
                        subtotal, discount_total, tax_total, grand_total,
                        payment_method, amount_paid, balance_due, payment_status, notes, business_id
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        invoice_no,
                        customer_id,
                        st.session_state.user["id"],
                        float(totals["grand_total"]),
                        legacy_status,
                        float(totals["subtotal"]),
                        float(totals["discount_total"]),
                        float(totals["tax_total"]),
                        float(totals["grand_total"]),
                        payment_method,
                        float(amount_paid),
                        float(balance_due),
                        payment_status,
                        notes.strip(),
                        bid,
                    ),
                )
                sale_id = sale_cur.lastrowid

                for line in cart:
                    conn_db.execute(
                        """
                        INSERT INTO sale_items(
                            sale_id, item_id, qty, unit_price, line_total,
                            sku, item_name, line_subtotal, line_discount, line_tax, discount_pct, tax_pct, business_id
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            int(sale_id),
                            int(line["item_id"]),
                            int(line["qty"]),
                            float(line["unit_price"]),
                            float(line["line_total"]),
                            str(line["sku"]),
                            str(line["item_name"]),
                            float(line["line_subtotal"]),
                            float(line["line_discount"]),
                            float(line["line_tax"]),
                            float(line["discount_pct"]),
                            float(line["tax_pct"]),
                            bid,
                        ),
                    )
                    conn_db.execute(
                        "UPDATE items SET stock_qty=stock_qty-? WHERE id=? AND business_id=?",
                        (int(line["qty"]), int(line["item_id"]), bid),
                    )
                    conn_db.execute(
                        "INSERT INTO stock_movements(item_id,movement_type,qty,reference,notes,created_by,business_id) VALUES(?,?,?,?,?,?,?)",
                        (int(line["item_id"]), "OUT", int(line["qty"]), invoice_no, "Sale transaction", st.session_state.user["id"], bid),
                    )

                conn_db.commit()
                st.session_state.last_invoice = invoice_no
                st.session_state.sales_cart = []
                log("CREATE_SALE", "sales", sale_id, f"{invoice_no} | {payment_status} | method={payment_method}")
                st.success(f"Sale finalized: {invoice_no}")
            except ValueError as ex:
                conn_db.rollback()
                st.error(str(ex))
            finally:
                conn_db.close()

    st.markdown("#### Recent Sales")
    sales_df = df(
        """
        SELECT
            invoice_no,
            COALESCE(grand_total, total_value) AS grand_total,
            payment_method,
            COALESCE(payment_status, status) AS payment_status,
            COALESCE(balance_due, 0) AS balance_due,
            status,
            created_at
        FROM sales
        WHERE business_id=:biz_id
        ORDER BY id DESC
        LIMIT 100
        """,
        {"biz_id": bid},
    )
    st.dataframe(sales_df, use_container_width=True, hide_index=True)

    st.markdown("#### Receipt / Transaction Output")
    if sales_df.empty:
        st.info("No sales receipts available yet.")
    else:
        invoice_options = sales_df["invoice_no"].astype(str).tolist()
        last_inv = st.session_state.get("last_invoice")
        default_idx = invoice_options.index(last_inv) if last_inv in invoice_options else 0
        inv = st.selectbox("Select Invoice", invoice_options, index=default_idx, key="receipt_invoice_select")
        receipt_pdf = create_sale_pdf(inv)
        r1, r2 = st.columns(2)
        with r1:
            st.download_button(
                "Download Receipt PDF",
                receipt_pdf,
                file_name=f"{inv}.pdf",
                mime="application/pdf",
                key=f"download_receipt_{inv}",
            )
        with r2:
            if st.button("Print Receipt (Default Printer)", key=f"print_receipt_{inv}", use_container_width=True):
                ok, msg = print_pdf_to_default_printer(receipt_pdf, inv)
                if ok:
                    log("PRINT_RECEIPT", "sales", None, f"Invoice {inv} sent to default printer")
                    st.success(msg)
                else:
                    st.error(msg)
        st.caption("For mini digital printers: set the mini printer as the Windows default printer and use paper size supported by your printer driver.")


def page_orders():
    bid = require_business_id()
    st.header("🧾 Orders Management")
    st.caption("Standardized workflow for made-to-order, custom service, and production tracking.")
    is_manager = has_manager_access()
    if "order_cart" not in st.session_state:
        st.session_state.order_cart = []

    customers = df("SELECT id,full_name FROM customers WHERE business_id=:biz_id ORDER BY full_name", {"biz_id": bid})
    users_df = df(
        "SELECT id,username,role FROM users WHERE business_id=:biz_id AND is_active=1 ORDER BY role DESC, username ASC",
        {"biz_id": bid},
    )
    items_df = df(
        "SELECT id, sku, name, unit_price FROM items WHERE business_id=:biz_id AND is_active=1 ORDER BY name",
        {"biz_id": bid},
    )

    customer_options = {"No customer": None}
    customer_options.update({row["full_name"]: int(row["id"]) for _, row in customers.iterrows()})
    assignee_options = {"Unassigned": None}
    assignee_options.update({f"{row['username']} ({row['role']})": int(row["id"]) for _, row in users_df.iterrows()})

    tab_create, tab_manage, tab_docs = st.tabs(["Create Order", "Manage Orders", "Documents"])

    with tab_create:
        with st.expander("Order Line Items", expanded=True):
            line_source = st.radio("Line Source", ["Catalog Item", "Custom Item"], horizontal=True, key="order_line_source")
            if line_source == "Catalog Item" and not items_df.empty:
                item_labels = [f"{r['sku']} - {r['name']}" for _, r in items_df.iterrows()]
                idx = st.selectbox("Catalog Item", range(len(item_labels)), format_func=lambda i: item_labels[i], key="order_line_catalog_pick")
                selected_item = items_df.iloc[idx]
                default_name = str(selected_item["name"])
                default_sku = str(selected_item["sku"])
                default_price = float(selected_item["unit_price"])
                selected_item_id = int(selected_item["id"])
            else:
                default_name = ""
                default_sku = ""
                default_price = 0.0
                selected_item_id = None

            l1, l2, l3, l4 = st.columns(4)
            with l1:
                line_name = st.text_input("Item Name", value=default_name, key="order_line_name")
            with l2:
                line_sku = st.text_input("SKU", value=default_sku, key="order_line_sku")
            with l3:
                line_qty = st.number_input("Qty", min_value=1, value=1, step=1, key="order_line_qty")
            with l4:
                line_unit_price = st.number_input("Unit Price", min_value=0.0, value=float(default_price), step=0.5, key="order_line_unit_price")
            line_notes = st.text_input("Line Notes", key="order_line_notes")

            c_add, c_remove, c_clear = st.columns(3)
            with c_add:
                if st.button("Add Line", use_container_width=True, key="order_add_line"):
                    if not line_name.strip():
                        st.error("Line item name is required.")
                    else:
                        line_total = round(float(line_qty) * float(line_unit_price), 2)
                        existing = next(
                            (
                                x
                                for x in st.session_state.order_cart
                                if x["item_name"].strip().lower() == line_name.strip().lower()
                                and (x.get("sku") or "").strip().lower() == line_sku.strip().lower()
                            ),
                            None,
                        )
                        if existing:
                            existing["qty"] = int(existing["qty"]) + int(line_qty)
                            existing["unit_price"] = float(line_unit_price)
                            existing["line_total"] = round(float(existing["qty"]) * float(existing["unit_price"]), 2)
                            existing["notes"] = line_notes.strip()
                            st.success("Line updated in order cart.")
                        else:
                            st.session_state.order_cart.append(
                                {
                                    "item_id": selected_item_id,
                                    "item_name": line_name.strip(),
                                    "sku": line_sku.strip(),
                                    "qty": int(line_qty),
                                    "unit_price": float(line_unit_price),
                                    "line_total": line_total,
                                    "notes": line_notes.strip(),
                                }
                            )
                            st.success("Line added to order cart.")

            if st.session_state.order_cart:
                with c_remove:
                    remove_labels = [f"{x['item_name']} [{x.get('sku') or '-'}]" for x in st.session_state.order_cart]
                    remove_idx = st.selectbox("Remove Line", range(len(remove_labels)), format_func=lambda i: remove_labels[i], key="order_remove_line_pick")
                    if st.button("Remove Selected", use_container_width=True, key="order_remove_selected"):
                        del st.session_state.order_cart[int(remove_idx)]
                        st.rerun()
                with c_clear:
                    if st.button("Clear Lines", use_container_width=True, key="order_clear_cart"):
                        st.session_state.order_cart = []
                        st.rerun()

            if st.session_state.order_cart:
                cart_df = pd.DataFrame(st.session_state.order_cart)[["item_name", "sku", "qty", "unit_price", "line_total", "notes"]]
                st.dataframe(cart_df, use_container_width=True, hide_index=True)
                order_cart_total = round(float(cart_df["line_total"].sum()), 2)
                st.caption(f"Order Cart Total: {order_cart_total:.2f}")
            else:
                order_cart_total = 0.0
                st.info("No line items yet. You can still create a single-description order.")

        with st.form("order_add_refined"):
            c1, c2, c3 = st.columns(3)
            with c1:
                c_sel = st.selectbox("Customer", list(customer_options.keys()))
                order_type = st.selectbox("Order Type", ["DRESS_TO_BE_MADE", "ALTERATION", "CUSTOM", "RESTOCK"])
                priority = st.selectbox("Priority", ["LOW", "MEDIUM", "HIGH", "URGENT"], index=1)
            with c2:
                due = st.date_input("Due Date", value=date.today())
                assignee = st.selectbox("Assign To", list(assignee_options.keys()))
                status = st.selectbox("Initial Status", ["PENDING", "IN_PROGRESS", "COMPLETED", "CANCELLED"])
            with c3:
                fallback_qty = st.number_input("Fallback Qty", min_value=1, value=1, step=1)
                fallback_unit_price = st.number_input("Fallback Unit Price", min_value=0.0, value=0.0, step=0.5)
                total_default = order_cart_total if order_cart_total > 0 else float(fallback_qty) * float(fallback_unit_price)
                total_value = st.number_input("Total Value", min_value=0.0, value=float(total_default), step=0.5)

            description = st.text_area("Order Description")
            notes = st.text_area("Internal Notes")
            submit_create = st.form_submit_button("Create Order")

        if submit_create:
            if not description.strip():
                st.error("Description is required.")
            else:
                c = conn()
                order_no = f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:5].upper()}"
                cart_lines = st.session_state.order_cart.copy()
                final_total = order_cart_total if order_cart_total > 0 else float(total_value)
                final_qty = int(sum(int(x["qty"]) for x in cart_lines)) if cart_lines else int(fallback_qty)
                final_unit_price = (float(final_total) / float(final_qty)) if final_qty > 0 else float(fallback_unit_price)
                cur = c.execute(
                    """
                    INSERT INTO orders(
                        order_no, customer_id, description, status, due_date, total_value, created_by,
                        order_type, priority, progress_pct, assigned_to, updated_at, notes, qty, unit_price, business_id
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        order_no,
                        customer_options[c_sel],
                        description.strip(),
                        status,
                        due.isoformat(),
                        float(final_total),
                        st.session_state.user["id"],
                        order_type,
                        priority,
                        100 if status == "COMPLETED" else 0,
                        assignee_options[assignee],
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        notes.strip(),
                        int(final_qty),
                        float(final_unit_price),
                        bid,
                    ),
                )
                order_id = cur.lastrowid
                if cart_lines:
                    for ln in cart_lines:
                        c.execute(
                            """
                            INSERT INTO order_items(order_id, item_id, item_name, sku, qty, unit_price, line_total, notes, business_id)
                            VALUES(?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                int(order_id),
                                ln.get("item_id"),
                                str(ln["item_name"]),
                                str(ln.get("sku") or ""),
                                int(ln["qty"]),
                                float(ln["unit_price"]),
                                float(ln["line_total"]),
                                str(ln.get("notes") or ""),
                                bid,
                            ),
                        )
                else:
                    c.execute(
                        """
                        INSERT INTO order_items(order_id, item_name, sku, qty, unit_price, line_total, notes, business_id)
                        VALUES(?,?,?,?,?,?,?,?)
                        """,
                        (
                            int(order_id),
                            description.strip()[:80],
                            "",
                            int(fallback_qty),
                            float(fallback_unit_price),
                            float(final_total),
                            notes.strip(),
                            bid,
                        ),
                    )
                c.commit()
                c.close()
                st.session_state.last_order = order_no
                st.session_state.order_cart = []
                log("CREATE_ORDER", "orders", order_id, f"{order_no} | {order_type} | {priority} | lines={len(cart_lines) if cart_lines else 1}")
                st.success(f"Order created: {order_no}")

    with tab_manage:
        all_orders = df(
            """
            SELECT
                o.id,
                o.order_no,
                COALESCE(c.full_name,'No customer') AS customer,
                o.description,
                o.status,
                o.due_date,
                o.total_value,
                o.created_at,
                COALESCE(o.updated_at, o.created_at) AS updated_at,
                COALESCE(o.order_type, 'DRESS_TO_BE_MADE') AS order_type,
                COALESCE(o.priority, 'MEDIUM') AS priority,
                COALESCE(o.progress_pct, 0) AS progress_pct,
                COALESCE(o.notes, '') AS notes,
                COALESCE(o.qty, 1) AS qty,
                COALESCE(o.unit_price, 0) AS unit_price,
                o.assigned_to,
                COALESCE(u.username,'Unassigned') AS assigned_to_user,
                (SELECT COUNT(*) FROM order_items oi WHERE oi.order_id=o.id AND oi.business_id=:biz_id) AS line_count
            FROM orders o
            LEFT JOIN customers c ON c.id=o.customer_id
            LEFT JOIN users u ON u.id=o.assigned_to
            WHERE o.business_id=:biz_id
            ORDER BY o.id DESC
            """,
            {"biz_id": bid},
        )

        status_filter, priority_filter, assignee_filter, overdue_only = st.columns(4)
        with status_filter:
            f_status = st.selectbox("Status", ["ALL", "PENDING", "IN_PROGRESS", "COMPLETED", "CANCELLED"])
        with priority_filter:
            f_priority = st.selectbox("Priority", ["ALL", "LOW", "MEDIUM", "HIGH", "URGENT"])
        with assignee_filter:
            assignees = ["ALL"] + sorted(all_orders["assigned_to_user"].dropna().unique().tolist()) if not all_orders.empty else ["ALL"]
            f_assignee = st.selectbox("Assigned To", assignees)
        with overdue_only:
            f_overdue = st.checkbox("Overdue only", value=False)

        q = st.text_input("Search orders", placeholder="Order number, customer, description")
        filtered = all_orders.copy()
        if f_status != "ALL":
            filtered = filtered[filtered["status"] == f_status]
        if f_priority != "ALL":
            filtered = filtered[filtered["priority"] == f_priority]
        if f_assignee != "ALL":
            filtered = filtered[filtered["assigned_to_user"] == f_assignee]
        if q.strip():
            needle = q.strip().lower()
            filtered = filtered[
                filtered["order_no"].str.lower().str.contains(needle, na=False)
                | filtered["customer"].str.lower().str.contains(needle, na=False)
                | filtered["description"].str.lower().str.contains(needle, na=False)
            ]
        if f_overdue and not filtered.empty:
            due_dt = pd.to_datetime(filtered["due_date"], errors="coerce")
            today = pd.Timestamp(date.today())
            filtered = filtered[(due_dt < today) & (~filtered["status"].isin(["COMPLETED", "CANCELLED"]))]

        total_cnt = int(len(all_orders))
        open_cnt = int(len(all_orders[all_orders["status"].isin(["PENDING", "IN_PROGRESS"])])) if not all_orders.empty else 0
        overdue_cnt = 0
        if not all_orders.empty:
            all_due = pd.to_datetime(all_orders["due_date"], errors="coerce")
            today = pd.Timestamp(date.today())
            overdue_cnt = int(((all_due < today) & (~all_orders["status"].isin(["COMPLETED", "CANCELLED"]))).sum())
        completed_cnt = int(len(all_orders[all_orders["status"] == "COMPLETED"])) if not all_orders.empty else 0
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Orders", total_cnt)
        k2.metric("Open Orders", open_cnt)
        k3.metric("Overdue Orders", overdue_cnt)
        k4.metric("Completed", completed_cnt)

        show_cols = ["order_no", "customer", "order_type", "priority", "status", "progress_pct", "line_count", "assigned_to_user", "due_date", "total_value", "updated_at"]
        st.dataframe(filtered[show_cols] if not filtered.empty else filtered, use_container_width=True, hide_index=True)

        if not filtered.empty:
            pick = st.selectbox("Select order to update", filtered["order_no"].tolist())
            row = filtered[filtered["order_no"] == pick].iloc[0]
            with st.form("order_update_form"):
                u1, u2, u3 = st.columns(3)
                with u1:
                    new_status = st.selectbox("Status", ["PENDING", "IN_PROGRESS", "COMPLETED", "CANCELLED"], index=["PENDING", "IN_PROGRESS", "COMPLETED", "CANCELLED"].index(str(row["status"])))
                    new_priority = st.selectbox("Priority", ["LOW", "MEDIUM", "HIGH", "URGENT"], index=["LOW", "MEDIUM", "HIGH", "URGENT"].index(str(row["priority"]) if str(row["priority"]) in ["LOW", "MEDIUM", "HIGH", "URGENT"] else "MEDIUM"))
                    new_order_type = st.selectbox("Order Type", ["DRESS_TO_BE_MADE", "ALTERATION", "CUSTOM", "RESTOCK"], index=["DRESS_TO_BE_MADE", "ALTERATION", "CUSTOM", "RESTOCK"].index(str(row["order_type"]) if str(row["order_type"]) in ["DRESS_TO_BE_MADE", "ALTERATION", "CUSTOM", "RESTOCK"] else "DRESS_TO_BE_MADE"))
                with u2:
                    new_due = st.date_input("Due Date", value=pd.to_datetime(row["due_date"]).date() if pd.notna(pd.to_datetime(row["due_date"], errors="coerce")) else date.today(), key=f"upd_due_{row['id']}")
                    new_progress = st.slider("Progress %", 0, 100, int(row["progress_pct"]), 5, key=f"upd_prog_{row['id']}")
                    new_assignee = st.selectbox("Assigned To", list(assignee_options.keys()), index=(list(assignee_options.values()).index(row["assigned_to"]) if row["assigned_to"] in list(assignee_options.values()) else 0), key=f"upd_asg_{row['id']}")
                with u3:
                    new_qty = st.number_input("Quantity", min_value=1, value=int(row["qty"]), step=1, key=f"upd_qty_{row['id']}")
                    new_unit_price = st.number_input("Unit Price", min_value=0.0, value=float(row["unit_price"]), step=0.5, key=f"upd_up_{row['id']}")
                    new_total = st.number_input("Total Value", min_value=0.0, value=float(row["total_value"]), step=0.5, key=f"upd_total_{row['id']}")

                new_desc = st.text_area("Description", value=str(row["description"]), key=f"upd_desc_{row['id']}")
                new_notes = st.text_area("Internal Notes", value=str(row["notes"]), key=f"upd_notes_{row['id']}")
                update_ok = st.form_submit_button("Save Updates")

            if update_ok:
                final_progress = 100 if new_status == "COMPLETED" else int(new_progress)
                c = conn()
                c.execute(
                    """
                    UPDATE orders
                    SET
                        status=?,
                        due_date=?,
                        total_value=?,
                        description=?,
                        order_type=?,
                        priority=?,
                        progress_pct=?,
                        assigned_to=?,
                        updated_at=?,
                        notes=?,
                        qty=?,
                        unit_price=?
                    WHERE id=? AND business_id=?
                    """,
                    (
                        new_status,
                        new_due.isoformat(),
                        float(new_total),
                        new_desc.strip(),
                        new_order_type,
                        new_priority,
                        int(final_progress),
                        assignee_options[new_assignee],
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        new_notes.strip(),
                        int(new_qty),
                        float(new_unit_price),
                        int(row["id"]),
                        bid,
                    ),
                )
                c.commit()
                c.close()
                log("UPDATE_ORDER", "orders", int(row["id"]), f"{row['order_no']} -> {new_status}, {final_progress}%")
                st.success("Order updated.")
                st.rerun()

            if is_manager:
                st.markdown("#### Order Deletion")
                delete_confirm = st.checkbox("Confirm order deletion", key=f"confirm_order_delete_{row['id']}")
                if st.button("Delete Order", key=f"delete_order_btn_{row['id']}", type="secondary"):
                    if not delete_confirm:
                        st.warning("Confirm deletion first.")
                    else:
                        delete_order_record(int(row["id"]))
                        log("DELETE_ORDER", "orders", int(row["id"]), f"Deleted order {row['order_no']}")
                        st.success(f"Order {row['order_no']} deleted.")
                        st.rerun()

            st.markdown("#### Selected Order Line Items")
            line_df = df(
                """
                SELECT id, item_name, sku, qty, unit_price, line_total, notes
                FROM order_items
                WHERE order_id=? AND business_id=?
                ORDER BY id ASC
                """,
                (int(row["id"]), bid),
            )
            if line_df.empty:
                st.info("No line items saved for this order.")
            else:
                st.dataframe(line_df, use_container_width=True, hide_index=True)

            with st.expander("Edit Order Lines", expanded=False):
                source_labels = [f"{r['sku']} - {r['name']}" for _, r in items_df.iterrows()] if not items_df.empty else []
                source_idx = st.selectbox("Catalog item", range(len(source_labels)) if source_labels else [0], format_func=(lambda i: source_labels[i]) if source_labels else (lambda i: "No catalog items"), key=f"upd_order_line_catalog_{row['id']}")
                if source_labels:
                    selected_cat = items_df.iloc[source_idx]
                    default_l_name = str(selected_cat["name"])
                    default_l_sku = str(selected_cat["sku"])
                    default_l_price = float(selected_cat["unit_price"])
                    default_item_id = int(selected_cat["id"])
                else:
                    default_l_name = ""
                    default_l_sku = ""
                    default_l_price = 0.0
                    default_item_id = None

                e1, e2, e3, e4 = st.columns(4)
                with e1:
                    upd_l_name = st.text_input("Item Name", value=default_l_name, key=f"upd_l_name_{row['id']}")
                with e2:
                    upd_l_sku = st.text_input("SKU", value=default_l_sku, key=f"upd_l_sku_{row['id']}")
                with e3:
                    upd_l_qty = st.number_input("Qty", min_value=1, value=1, step=1, key=f"upd_l_qty_{row['id']}")
                with e4:
                    upd_l_price = st.number_input("Unit Price", min_value=0.0, value=default_l_price, step=0.5, key=f"upd_l_price_{row['id']}")
                upd_l_notes = st.text_input("Line Notes", key=f"upd_l_notes_{row['id']}")
                a1, a2 = st.columns(2)
                with a1:
                    if st.button("Add Line To Order", use_container_width=True, key=f"add_line_order_{row['id']}"):
                        if not upd_l_name.strip():
                            st.error("Line name is required.")
                        else:
                            c = conn()
                            c.execute(
                                """
                                INSERT INTO order_items(order_id, item_id, item_name, sku, qty, unit_price, line_total, notes, business_id)
                                VALUES(?,?,?,?,?,?,?,?,?)
                                """,
                                (
                                    int(row["id"]),
                                    default_item_id,
                                    upd_l_name.strip(),
                                    upd_l_sku.strip(),
                                    int(upd_l_qty),
                                    float(upd_l_price),
                                    round(float(upd_l_qty) * float(upd_l_price), 2),
                                    upd_l_notes.strip(),
                                    bid,
                                ),
                            )
                            c.commit()
                            c.close()
                            recalc_order_totals(int(row["id"]))
                            log("ORDER_LINE_ADD", "orders", int(row["id"]), f"line={upd_l_name.strip()}")
                            st.success("Line item added.")
                            st.rerun()
                with a2:
                    if not line_df.empty:
                        remove_line_id = st.selectbox("Remove line", line_df["id"].tolist(), key=f"rm_line_sel_{row['id']}")
                        if st.button("Remove Selected Line", use_container_width=True, key=f"remove_line_order_{row['id']}"):
                            c = conn()
                            c.execute(
                                "DELETE FROM order_items WHERE id=? AND order_id=? AND business_id=?",
                                (int(remove_line_id), int(row["id"]), bid),
                            )
                            c.commit()
                            c.close()
                            recalc_order_totals(int(row["id"]))
                            log("ORDER_LINE_REMOVE", "orders", int(row["id"]), f"line_id={int(remove_line_id)}")
                            st.success("Line item removed.")
                            st.rerun()

    with tab_docs:
        docs_df = df(
            """
            SELECT
                o.order_no,
                o.status,
                o.due_date,
                o.total_value,
                (SELECT COUNT(*) FROM order_items oi WHERE oi.order_id=o.id AND oi.business_id=:biz_id) AS line_count
            FROM orders o
            WHERE o.business_id=:biz_id
            ORDER BY o.id DESC
            """,
            {"biz_id": bid},
        )
        if docs_df.empty:
            st.info("No orders available.")
        else:
            st.dataframe(docs_df, use_container_width=True, hide_index=True)
            pick_doc = st.selectbox("Select order", docs_df["order_no"].tolist(), key="order_doc_pick")
            st.download_button("Download Professional Order PDF", create_order_pdf(pick_doc), file_name=f"{pick_doc}.pdf", mime="application/pdf")


def page_search():
    bid = require_business_id()
    st.header("🔎 Records Explorer")
    st.caption("Unified lookup across fashion items, customers, orders, order lines, sales, and activity logs.")

    scopes = ["Fashion Items", "Customers", "Orders", "Order Lines", "Sales", "Activity"]
    c1, c2 = st.columns([2.2, 1])
    with c1:
        q = st.text_input("Search keyword", placeholder="SKU, customer name, invoice, order number, phone, status...")
    with c2:
        selected_scopes = st.multiselect("Search In", scopes, default=scopes)

    if not q.strip():
        st.info("Enter a keyword to explore records.")
        snap = df(
            """
            SELECT
                (SELECT COUNT(*) FROM items WHERE business_id=:biz_id) AS items,
                (SELECT COUNT(*) FROM customers WHERE business_id=:biz_id) AS customers,
                (SELECT COUNT(*) FROM orders WHERE business_id=:biz_id) AS orders,
                (SELECT COUNT(*) FROM order_items WHERE business_id=:biz_id) AS order_lines,
                (SELECT COUNT(*) FROM sales WHERE business_id=:biz_id) AS sales,
                (SELECT COUNT(*) FROM activity_logs WHERE business_id=:biz_id) AS logs
            """,
            {"biz_id": bid},
        ).iloc[0]
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        k1.metric("Fashion Items", int(snap["items"]))
        k2.metric("Customers", int(snap["customers"]))
        k3.metric("Orders", int(snap["orders"]))
        k4.metric("Order Lines", int(snap["order_lines"]))
        k5.metric("Sales", int(snap["sales"]))
        k6.metric("Logs", int(snap["logs"]))
        return

    like = f"%{q.strip()}%"
    results = {}

    if "Fashion Items" in selected_scopes:
        results["Fashion Items"] = df(
            """
            SELECT id, sku, name, category, stock_qty, unit_price, reorder_level, created_at
            FROM items
            WHERE business_id=:biz_id AND (sku LIKE :like OR name LIKE :like OR category LIKE :like)
            ORDER BY id DESC
            """,
            {"biz_id": bid, "like": like},
        )
    if "Customers" in selected_scopes:
        results["Customers"] = df(
            """
            SELECT id, full_name, phone, email, measurement_unit, hand_length, chest_width, waist_length, shoulder_length
            FROM customers
            WHERE business_id=:biz_id AND (full_name LIKE :like OR phone LIKE :like OR email LIKE :like OR address LIKE :like)
            ORDER BY id DESC
            """,
            {"biz_id": bid, "like": like},
        )
    if "Orders" in selected_scopes:
        results["Orders"] = df(
            """
            SELECT
                o.order_no,
                o.status,
                o.priority,
                o.order_type,
                o.due_date,
                o.total_value,
                COALESCE(c.full_name,'No customer') AS customer,
                COALESCE(u.username,'Unassigned') AS assigned_to,
                (SELECT COUNT(*) FROM order_items oi WHERE oi.order_id=o.id AND oi.business_id=:biz_id) AS line_count
            FROM orders o
            LEFT JOIN customers c ON c.id=o.customer_id
            LEFT JOIN users u ON u.id=o.assigned_to
            WHERE
                o.business_id=:biz_id AND (
                    o.order_no LIKE :like
                    OR o.description LIKE :like
                    OR o.status LIKE :like
                    OR o.priority LIKE :like
                    OR o.order_type LIKE :like
                    OR COALESCE(c.full_name,'') LIKE :like
                )
            ORDER BY o.id DESC
            """,
            {"biz_id": bid, "like": like},
        )
    if "Order Lines" in selected_scopes:
        results["Order Lines"] = df(
            """
            SELECT
                o.order_no,
                oi.item_name,
                oi.sku,
                oi.qty,
                oi.unit_price,
                oi.line_total,
                oi.notes
            FROM order_items oi
            JOIN orders o ON o.id=oi.order_id
            WHERE
                o.business_id=:biz_id
                AND oi.business_id=:biz_id
                AND (
                    oi.item_name LIKE :like
                    OR oi.sku LIKE :like
                    OR oi.notes LIKE :like
                    OR o.order_no LIKE :like
                )
            ORDER BY oi.id DESC
            """,
            {"biz_id": bid, "like": like},
        )
    if "Sales" in selected_scopes:
        results["Sales"] = df(
            """
            SELECT
                s.invoice_no,
                COALESCE(c.full_name,'Walk-in') AS customer,
                COALESCE(u.username,'N/A') AS sales_rep,
                COALESCE(s.grand_total, s.total_value) AS grand_total,
                COALESCE(s.payment_method,'N/A') AS payment_method,
                COALESCE(s.payment_status, s.status) AS payment_status,
                s.created_at
            FROM sales s
            LEFT JOIN customers c ON c.id=s.customer_id
            LEFT JOIN users u ON u.id=s.sales_rep_id
            WHERE
                s.business_id=:biz_id AND (
                    s.invoice_no LIKE :like
                    OR COALESCE(c.full_name,'') LIKE :like
                    OR COALESCE(u.username,'') LIKE :like
                    OR COALESCE(s.payment_method,'') LIKE :like
                    OR COALESCE(s.payment_status,'') LIKE :like
                )
            ORDER BY s.id DESC
            """,
            {"biz_id": bid, "like": like},
        )
    if "Activity" in selected_scopes:
        results["Activity"] = df(
            """
            SELECT
                l.created_at,
                COALESCE(u.username,'-') AS username,
                l.action,
                l.entity_type,
                l.entity_id,
                l.details
            FROM activity_logs l
            LEFT JOIN users u ON u.id=l.user_id
            WHERE
                l.business_id=:biz_id AND (
                    l.action LIKE :like
                    OR l.entity_type LIKE :like
                    OR COALESCE(l.details,'') LIKE :like
                    OR COALESCE(u.username,'') LIKE :like
                )
            ORDER BY l.id DESC
            LIMIT 300
            """,
            {"biz_id": bid, "like": like},
        )

    st.markdown("#### Result Summary")
    sum_cols = st.columns(max(1, len(results)))
    for idx, (name, frame) in enumerate(results.items()):
        sum_cols[idx].metric(name, int(len(frame)))

    st.markdown("#### Result Sets")
    for name, frame in results.items():
        with st.expander(f"{name} ({len(frame)})", expanded=(len(frame) > 0)):
            if frame.empty:
                st.info(f"No {name.lower()} matched this keyword.")
            else:
                st.dataframe(frame, use_container_width=True, hide_index=True)


def page_history():
    bid = require_business_id()
    st.header("🕘 Activity Logs")
    st.caption("System audit trail for operational events, user actions, and record-level changes.")
    logs_df = df(
        """
        SELECT
            l.id,
            l.created_at,
            COALESCE(u.username, '-') AS username,
            l.action,
            l.entity_type,
            l.entity_id,
            l.details
        FROM activity_logs l
        LEFT JOIN users u ON u.id=l.user_id
        WHERE l.business_id=:biz_id
        ORDER BY l.id DESC
        LIMIT 2000
        """,
        {"biz_id": bid},
    )
    if logs_df.empty:
        st.info("No activity logs recorded yet.")
        return

    logs_df["date"] = pd.to_datetime(logs_df["created_at"], errors="coerce").dt.date
    min_date = logs_df["date"].min()
    max_date = logs_df["date"].max()

    f1, f2, f3, f4 = st.columns([1, 1, 1.2, 1.6])
    with f1:
        from_d = st.date_input("From", value=min_date if pd.notna(min_date) else date.today(), key="logs_from")
    with f2:
        to_d = st.date_input("To", value=max_date if pd.notna(max_date) else date.today(), key="logs_to")
    with f3:
        users = sorted(logs_df["username"].dropna().astype(str).unique().tolist())
        user_pick = st.multiselect("Users", users, default=users, key="logs_users")
    with f4:
        actions = sorted(logs_df["action"].dropna().astype(str).unique().tolist())
        action_pick = st.multiselect("Actions", actions, default=actions, key="logs_actions")

    keyword = st.text_input("Search logs", placeholder="Action, user, entity type, details...", key="logs_keyword")
    filtered = logs_df.copy()
    if from_d > to_d:
        st.error("From date must be before To date.")
        return
    filtered = filtered[(filtered["date"] >= from_d) & (filtered["date"] <= to_d)]
    if user_pick:
        filtered = filtered[filtered["username"].astype(str).isin(user_pick)]
    if action_pick:
        filtered = filtered[filtered["action"].astype(str).isin(action_pick)]
    if keyword.strip():
        needle = keyword.strip().lower()
        filtered = filtered[
            filtered["username"].astype(str).str.lower().str.contains(needle, na=False)
            | filtered["action"].astype(str).str.lower().str.contains(needle, na=False)
            | filtered["entity_type"].astype(str).str.lower().str.contains(needle, na=False)
            | filtered["details"].astype(str).str.lower().str.contains(needle, na=False)
        ]

    k1, k2, k3 = st.columns(3)
    k1.metric("Log Entries", int(len(filtered)))
    k2.metric("Users", int(filtered["username"].nunique()) if not filtered.empty else 0)
    k3.metric("Action Types", int(filtered["action"].nunique()) if not filtered.empty else 0)

    view_df = filtered[["id", "created_at", "username", "action", "entity_type", "entity_id", "details"]]
    st.dataframe(view_df, use_container_width=True, hide_index=True)
    st.download_button(
        "Download Activity Logs CSV",
        view_df.to_csv(index=False).encode("utf-8"),
        file_name=f"activity_logs_{date.today().isoformat()}.csv",
        mime="text/csv",
    )


def page_reports():
    bid = require_business_id()
    st.header("📈 Reports & Analytics")
    st.caption("Operational and financial reporting workspace for sales, orders, and performance insights.")

    c1, c2, c3 = st.columns([1, 1, 1.2])
    with c1:
        d1 = st.date_input("From", value=date.today().replace(day=1), key="rep_from")
    with c2:
        d2 = st.date_input("To", value=date.today(), key="rep_to")
    with c3:
        preset = st.selectbox("Quick Range", ["Custom", "Today", "Last 7 Days", "Last 30 Days", "This Month"])
    if preset != "Custom":
        today = date.today()
        if preset == "Today":
            d1, d2 = today, today
        elif preset == "Last 7 Days":
            d1, d2 = today - pd.Timedelta(days=6), today
        elif preset == "Last 30 Days":
            d1, d2 = today - pd.Timedelta(days=29), today
        elif preset == "This Month":
            d1, d2 = today.replace(day=1), today
    if d1 > d2:
        st.error("From date must be before To date.")
        return
    start_dt = datetime.combine(d1, datetime.min.time())
    end_dt = datetime.combine(d2 + timedelta(days=1), datetime.min.time())
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    sales_raw = df(
        """
        SELECT
            s.id,
            s.invoice_no,
            COALESCE(c.full_name,'Walk-in') AS customer,
            COALESCE(u.username,'N/A') AS sales_rep,
            COALESCE(s.grand_total, s.total_value) AS grand_total,
            COALESCE(s.subtotal, s.total_value) AS subtotal,
            COALESCE(s.discount_total, 0) AS discount_total,
            COALESCE(s.tax_total, 0) AS tax_total,
            COALESCE(s.payment_method, 'N/A') AS payment_method,
            COALESCE(s.payment_status, s.status) AS payment_status,
            COALESCE(s.amount_paid, COALESCE(s.grand_total, s.total_value)) AS amount_paid,
            COALESCE(s.balance_due, 0) AS balance_due,
            s.created_at
        FROM sales s
        LEFT JOIN customers c ON c.id=s.customer_id
        LEFT JOIN users u ON u.id=s.sales_rep_id
        WHERE s.business_id=:biz_id AND s.created_at>=:start_str AND s.created_at<:end_str
        ORDER BY s.id DESC
        """,
        {"biz_id": bid, "start_str": start_str, "end_str": end_str},
    )
    orders_raw = df(
        """
        SELECT
            o.id,
            o.order_no,
            COALESCE(c.full_name,'No customer') AS customer,
            COALESCE(o.order_type,'DRESS_TO_BE_MADE') AS order_type,
            COALESCE(o.priority,'MEDIUM') AS priority,
            o.status,
            o.due_date,
            COALESCE(o.progress_pct,0) AS progress_pct,
            COALESCE(o.total_value,0) AS total_value,
            COALESCE(u.username,'Unassigned') AS assigned_to,
            (SELECT COUNT(*) FROM order_items oi WHERE oi.order_id=o.id AND oi.business_id=:biz_id) AS line_count,
            o.created_at
        FROM orders o
        LEFT JOIN customers c ON c.id=o.customer_id
        LEFT JOIN users u ON u.id=o.assigned_to
        WHERE o.business_id=:biz_id AND o.created_at>=:start_str AND o.created_at<:end_str
        ORDER BY o.id DESC
        """,
        {"biz_id": bid, "start_str": start_str, "end_str": end_str},
    )

    sales_statuses = sorted(sales_raw["payment_status"].dropna().astype(str).unique().tolist()) if not sales_raw.empty else []
    payment_methods = sorted(sales_raw["payment_method"].dropna().astype(str).unique().tolist()) if not sales_raw.empty else []
    order_statuses = sorted(orders_raw["status"].dropna().astype(str).unique().tolist()) if not orders_raw.empty else []
    order_priorities = sorted(orders_raw["priority"].dropna().astype(str).unique().tolist()) if not orders_raw.empty else []

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        payment_status_filter = st.multiselect("Payment Status", sales_statuses, default=sales_statuses)
    with f2:
        payment_method_filter = st.multiselect("Payment Method", payment_methods, default=payment_methods)
    with f3:
        order_status_filter = st.multiselect("Order Status", order_statuses, default=order_statuses)
    with f4:
        order_priority_filter = st.multiselect("Order Priority", order_priorities, default=order_priorities)

    sales = sales_raw.copy()
    if not sales.empty:
        if payment_status_filter:
            sales = sales[sales["payment_status"].isin(payment_status_filter)]
        if payment_method_filter:
            sales = sales[sales["payment_method"].isin(payment_method_filter)]
    orders = orders_raw.copy()
    if not orders.empty:
        if order_status_filter:
            orders = orders[orders["status"].isin(order_status_filter)]
        if order_priority_filter:
            orders = orders[orders["priority"].isin(order_priority_filter)]

    sales_count = int(len(sales))
    revenue = float(sales["grand_total"].sum()) if not sales.empty else 0.0
    outstanding = float(sales["balance_due"].sum()) if not sales.empty else 0.0
    avg_invoice = (revenue / sales_count) if sales_count > 0 else 0.0
    orders_count = int(len(orders))
    order_value = float(orders["total_value"].sum()) if not orders.empty else 0.0
    open_orders = int(len(orders[orders["status"].isin(["PENDING", "IN_PROGRESS"])])) if not orders.empty else 0
    overdue_orders = 0
    if not orders.empty:
        due = pd.to_datetime(orders["due_date"], errors="coerce")
        overdue_orders = int(((due < pd.Timestamp(date.today())) & (~orders["status"].isin(["COMPLETED", "CANCELLED"]))).sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sales Count", sales_count)
    k2.metric("Revenue", f"{revenue:,.2f}")
    k3.metric("Outstanding Balance", f"{outstanding:,.2f}")
    k4.metric("Avg Invoice", f"{avg_invoice:,.2f}")
    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Orders Count", orders_count)
    k6.metric("Orders Value", f"{order_value:,.2f}")
    k7.metric("Open Orders", open_orders)
    k8.metric("Overdue Orders", overdue_orders)

    tab_overview, tab_sales, tab_orders, tab_exports = st.tabs(["Overview", "Sales Insights", "Orders Insights", "Exports"])

    with tab_overview:
        left, right = st.columns(2)
        with left:
            st.markdown("#### Revenue Trend")
            if sales.empty:
                st.info("No sales data for selected filters.")
            else:
                sales_trend = sales.copy()
                sales_trend["day"] = pd.to_datetime(sales_trend["created_at"], errors="coerce").dt.date
                sales_trend = sales_trend.groupby("day", as_index=False)["grand_total"].sum().sort_values("day")
                st.plotly_chart(px.line(sales_trend, x="day", y="grand_total", markers=True, title="Daily Revenue"), use_container_width=True)
        with right:
            st.markdown("#### Orders Trend")
            if orders.empty:
                st.info("No orders data for selected filters.")
            else:
                order_trend = orders.copy()
                order_trend["day"] = pd.to_datetime(order_trend["created_at"], errors="coerce").dt.date
                order_trend = order_trend.groupby("day", as_index=False)["id"].count().rename(columns={"id": "orders_count"}).sort_values("day")
                st.plotly_chart(px.bar(order_trend, x="day", y="orders_count", title="Daily Orders"), use_container_width=True)

    with tab_sales:
        s_left, s_right = st.columns(2)
        with s_left:
            st.markdown("#### Top Customers by Revenue")
            if sales.empty:
                st.info("No sales data.")
            else:
                top_customers = sales.groupby("customer", as_index=False)["grand_total"].sum().sort_values("grand_total", ascending=False).head(10)
                st.plotly_chart(px.bar(top_customers, x="customer", y="grand_total", color="grand_total"), use_container_width=True)
        with s_right:
            st.markdown("#### Top Sales Reps")
            if sales.empty:
                st.info("No sales data.")
            else:
                top_reps = sales.groupby("sales_rep", as_index=False)["grand_total"].sum().sort_values("grand_total", ascending=False).head(10)
                st.plotly_chart(px.bar(top_reps, x="sales_rep", y="grand_total", color="grand_total"), use_container_width=True)

        st.markdown("#### Sales Register")
        st.dataframe(sales, use_container_width=True, hide_index=True)

    with tab_orders:
        o_left, o_right = st.columns(2)
        with o_left:
            st.markdown("#### Orders by Status")
            if orders.empty:
                st.info("No orders data.")
            else:
                status_df = orders.groupby("status", as_index=False)["id"].count().rename(columns={"id": "count"})
                st.plotly_chart(px.pie(status_df, names="status", values="count"), use_container_width=True)
        with o_right:
            st.markdown("#### Orders by Priority")
            if orders.empty:
                st.info("No orders data.")
            else:
                pr_df = orders.groupby("priority", as_index=False)["id"].count().rename(columns={"id": "count"})
                st.plotly_chart(px.bar(pr_df, x="priority", y="count", color="count"), use_container_width=True)

        st.markdown("#### Orders Register")
        st.dataframe(orders, use_container_width=True, hide_index=True)

    with tab_exports:
        st.markdown("#### Download Data")
        exp1, exp2, exp3 = st.columns(3)
        with exp1:
            st.download_button(
                "Download Sales CSV",
                sales.to_csv(index=False).encode("utf-8"),
                file_name=f"sales_report_{d1}_{d2}.csv",
                mime="text/csv",
            )
        with exp2:
            st.download_button(
                "Download Orders CSV",
                orders.to_csv(index=False).encode("utf-8"),
                file_name=f"orders_report_{d1}_{d2}.csv",
                mime="text/csv",
            )
        with exp3:
            outstanding_df = sales[sales["balance_due"] > 0].copy() if not sales.empty else pd.DataFrame()
            st.download_button(
                "Download Outstanding CSV",
                outstanding_df.to_csv(index=False).encode("utf-8"),
                file_name=f"outstanding_{d1}_{d2}.csv",
                mime="text/csv",
            )

        summary_df = pd.DataFrame(
            [
                {"metric": "sales_count", "value": sales_count},
                {"metric": "revenue", "value": revenue},
                {"metric": "outstanding_balance", "value": outstanding},
                {"metric": "avg_invoice", "value": avg_invoice},
                {"metric": "orders_count", "value": orders_count},
                {"metric": "orders_value", "value": order_value},
                {"metric": "open_orders", "value": open_orders},
                {"metric": "overdue_orders", "value": overdue_orders},
            ]
        )
        st.download_button(
            "Download KPI Summary CSV",
            summary_df.to_csv(index=False).encode("utf-8"),
            file_name=f"kpi_summary_{d1}_{d2}.csv",
            mime="text/csv",
        )


def main():
    st.set_page_config(page_title="CIGMA CRM System", layout="wide", page_icon="🏬")
    init_db()
    if "auth" not in st.session_state:
        st.session_state.auth = False
    if "user" not in st.session_state:
        st.session_state.user = None
    if "page" not in st.session_state:
        st.session_state.page = "Home"

    inject_styles(st.session_state.auth)

    if not st.session_state.auth:
        login_view()
        return

    u = st.session_state.user
    if str(u.get("role", "")).strip().lower() == "admin":
        u["role"] = "manager"
        st.session_state.user = u
    bid = require_business_id()
    manager_pages = ["Home", "Dashboard", "Sales", "Orders", "Customers", "Reports", "Support Centre", "Fashion Items", "Stock In", "Activity Logs", "User Management", "Records Explorer"]
    staff_pages = ["Home", "Dashboard", "Sales", "Orders", "Customers", "Support Centre", "Records Explorer"]
    pages = manager_pages if has_manager_access(u) else staff_pages
    if st.session_state.page == "Search":
        st.session_state.page = "Records Explorer"
    if st.session_state.page == "Items":
        st.session_state.page = "Fashion Items"
    if st.session_state.page == "History":
        st.session_state.page = "Activity Logs"
    if st.session_state.page == "Stock Out":
        st.session_state.page = "Stock In"
    if st.session_state.page not in pages:
        st.session_state.page = pages[0]

    with st.sidebar:
        st.title("🏬 CIGMA CRM System")
        st.caption(f"{u['username']} ({u['role']})")
        biz_df = df("SELECT id, name FROM businesses WHERE is_active=1 ORDER BY name")
        biz_labels = {int(r["id"]): str(r["name"]) for _, r in biz_df.iterrows()} if not biz_df.empty else {}
        if biz_labels:
            current_bid = int(st.session_state.get("business_id", bid))
            if current_bid not in biz_labels:
                current_bid = list(biz_labels.keys())[0]
                st.session_state.business_id = current_bid
            if has_manager_access(u):
                selected_bid = st.selectbox(
                    "Business",
                    list(biz_labels.keys()),
                    index=list(biz_labels.keys()).index(current_bid),
                    format_func=lambda x: biz_labels.get(int(x), f"Business {x}"),
                    key="business_switcher",
                )
                if int(selected_bid) != int(current_bid):
                    st.session_state.business_id = int(selected_bid)
                    st.rerun()
            else:
                st.caption(f"Business: {biz_labels.get(current_bid, 'Business')}")
        with st.expander("Profile Photo", expanded=False):
            current_photo = user_photo_data_uri(u)
            st.markdown(
                f"<div style='text-align:center;'><img src='{current_photo}' width='88' style='border-radius:50%;border:2px solid #fff6dc;'/></div>",
                unsafe_allow_html=True,
            )
            up = st.file_uploader("Upload photo", type=["png", "jpg", "jpeg", "webp"], key="profile_photo_uploader")
            if st.button("Save Photo", key="save_profile_photo", use_container_width=True):
                if up is None:
                    st.warning("Choose an image first.")
                else:
                    try:
                        save_user_photo(u["id"], up)
                        log("UPDATE_PROFILE_PHOTO", "users", u["id"], "Profile photo updated")
                        st.success("Photo updated.")
                        st.rerun()
                    except ValueError as ex:
                        st.error(str(ex))
        st.markdown("### 🧭 Navigation")
        nav_pages = [p for p in pages if p != "Support Centre"]
        for p in nav_pages:
            p_label = page_label(p)
            label = f"▶ {p_label}" if p == st.session_state.page else p_label
            if st.button(label, key=f"side_nav_{p}", use_container_width=True):
                st.session_state.page = p
                st.rerun()
        if "Support Centre" in pages:
            support_label = page_label("Support Centre")
            support_button_label = f"▶ {support_label}" if st.session_state.page == "Support Centre" else support_label
            if st.button(support_button_label, key="side_nav_support_centre", use_container_width=True):
                st.session_state.page = "Support Centre"
                st.rerun()
        st.divider()
        if st.button("Log out", key="side_logout", use_container_width=True):
            log("LOGOUT", "users", u["id"], "User logout")
            st.session_state.auth = False
            st.session_state.user = None
            st.session_state.business_id = None
            st.rerun()

    auto_collapse_sidebar_on_mobile()
    render_top_navigation(pages)
    page = st.session_state.page

    if page == "Home":
        page_home(u, pages)
    elif page == "Dashboard":
        page_dashboard()
    elif page == "Fashion Items":
        page_items()
    elif page == "Stock In":
        page_stock("IN")
    elif page == "Sales":
        page_sales()
    elif page == "Orders":
        page_orders()
    elif page == "Customers":
        page_customers()
    elif page == "Records Explorer":
        page_search()
    elif page == "Activity Logs":
        page_history()
    elif page == "User Management":
        page_user_management()
    elif page == "Reports":
        page_reports()
    elif page == "Support Centre":
        page_support()

    render_footer()


if __name__ == "__main__":
    main()
