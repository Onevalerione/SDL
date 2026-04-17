#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import html
import logging
import os
import re
import secrets
import traceback
from datetime import datetime
from decimal import Decimal, InvalidOperation
from http import cookies
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("APP_HOST", "0.0.0.0")
PORT = int(os.getenv("APP_PORT", "5000"))
APP_USER = os.getenv("APP_USER", "admin")
APP_PASSWORD = os.getenv("APP_PASSWORD", "admin123")
SESSION_SECRET = os.getenv("SESSION_SECRET", secrets.token_hex(16))
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "0") == "1"
LOG_DIR = os.getenv("LOG_DIR", "logs")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "ecommerce")
DB_USER = os.getenv("DB_USER", "app_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "secure_password_123")

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "auth.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

SESSIONS = {}

TABLE_CONFIG = {
    "categories": {
        "label": "Категории",
        "fields": ["name", "description"],
        "search": ["name", "description"],
        "display": ["name", "description", "created_at"],
        "title": "name",
    },
    "products": {
        "label": "Товары",
        "fields": ["name", "description", "category_id", "price", "stock"],
        "search": ["name", "description"],
        "display": ["name", "description", "category_name", "price", "stock", "created_at", "updated_at"],
        "title": "name",
    },
    "customers": {
        "label": "Клиенты",
        "fields": ["first_name", "last_name", "email", "phone", "address"],
        "search": ["first_name", "last_name", "email", "phone", "address"],
        "display": ["full_name", "email", "phone", "address", "created_at"],
        "title": "email",
    },
    "orders": {
        "label": "Заказы",
        "fields": ["customer_id", "status"],
        "search": ["status"],
        "display": ["customer_name", "order_date", "total_amount", "status"],
        "title": "customer_name",
    },
    "order_items": {
        "label": "Позиции заказов",
        "fields": ["order_id", "product_id", "quantity", "price"],
        "search": [],
        "display": ["order_label", "product_name", "quantity", "price"],
        "title": "product_name",
    },
}

STATUS_VALUES = ["pending", "shipped", "delivered", "cancelled"]
BATCH_LIMIT = 100


def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
    )


def esc(v):
    return html.escape("" if v is None else str(v))


def normalize_text(v, max_len=255):
    v = (v or "").strip()
    if not v:
        return None
    return v[:max_len]


def normalize_email(v):
    v = normalize_text(v, 255)
    if not v:
        return None
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", v):
        raise ValueError("Некорректный email")
    return v


def normalize_int(v, min_value=None):
    if v is None or str(v).strip() == "":
        return None
    value = int(str(v).strip())
    if min_value is not None and value < min_value:
        raise ValueError("Число меньше допустимого")
    return value


def normalize_decimal(v, min_value=None):
    if v is None or str(v).strip() == "":
        return None
    try:
        value = Decimal(str(v).strip())
    except InvalidOperation:
        raise ValueError("Некорректное число")
    if min_value is not None and value < Decimal(str(min_value)):
        raise ValueError("Значение меньше допустимого")
    return value


def parse_id_list(raw_values):
    values = []
    for raw in raw_values:
        for part in str(raw).split(","):
            part = part.strip()
            if not part:
                continue
            if not re.fullmatch(r"\d+", part):
                raise ValueError("Список содержит недопустимое значение")
            values.append(int(part))
    values = list(dict.fromkeys(values))
    if len(values) > BATCH_LIMIT:
        raise ValueError("Слишком много значений в группе")
    return values


def build_in_clause(values):
    if not values:
        raise ValueError("Пустая группа значений")
    return sql.SQL(", ").join([sql.Placeholder()] * len(values))


def app_user_valid(username, password):
    return secrets.compare_digest(username, APP_USER) and secrets.compare_digest(password, APP_PASSWORD)


def new_session(username):
    sid = secrets.token_urlsafe(32)
    SESSIONS[sid] = {"username": username, "created_at": datetime.utcnow().isoformat()}
    return sid


def page_template(title, body, user=None, flash=None):
    nav = ""
    if user:
        nav = """
        <nav class='nav'>
          <a href='/home'>Главная</a>
          <a href='/tables'>Таблицы</a>
          <a href='/add'>Добавление</a>
          <a href='/batch-add'>Пакетное добавление</a>
          <a href='/update'>Обновление</a>
          <a href='/batch-update'>Массовое обновление</a>
          <a href='/logout'>Выход</a>
        </nav>
        """
    flash_html = f"<div class='flash'>{esc(flash)}</div>" if flash else ""
    return f"""<!doctype html>
<html lang='ru'>
<head>
<meta charset='UTF-8'>
<meta name='viewport' content='width=device-width, initial-scale=1.0'>
<title>{esc(title)}</title>
<style>
:root {{
  --bg:#ffffff; --surface:#ffffff; --surface-2:#f8f7ff; --surface-3:#f2efff;
  --text:#1f1b2d; --muted:#6b6580; --border:#e7e1ff; --primary:#6d28d9; --primary-hover:#5b21b6;
  --danger:#be123c; --success:#166534; --shadow:0 14px 40px rgba(109,40,217,.10);
  --radius:18px; --radius-sm:12px;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family:Inter,Segoe UI,Arial,sans-serif; background:linear-gradient(180deg,#fff 0%,#fcfbff 100%); color:var(--text); }}
a {{ color:var(--primary); text-decoration:none; }}
.header {{ position:sticky; top:0; background:rgba(255,255,255,.92); backdrop-filter:blur(12px); border-bottom:1px solid var(--border); z-index:3; }}
.header-inner {{ max-width:1200px; margin:0 auto; padding:18px 24px; display:flex; justify-content:space-between; align-items:center; gap:16px; }}
.brand {{ font-weight:800; letter-spacing:.2px; color:var(--text); }}
.nav {{ display:flex; flex-wrap:wrap; gap:10px; }}
.nav a {{ color:var(--text); padding:10px 14px; border-radius:999px; }}
.nav a:hover {{ background:var(--surface-3); }}
.main {{ max-width:1200px; margin:32px auto; padding:0 20px 48px; }}
.hero, .card, .table-wrap {{ background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); box-shadow:var(--shadow); }}
.hero {{ padding:32px; margin-bottom:24px; }}
.hero h1 {{ margin:0 0 10px; font-size:36px; }}
.hero p {{ margin:0; color:var(--muted); max-width:70ch; line-height:1.6; }}
.grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(250px,1fr)); gap:18px; }}
.card {{ padding:22px; }}
.card h3 {{ margin:0 0 8px; font-size:20px; }}
.card p {{ margin:0 0 16px; color:var(--muted); line-height:1.55; }}
.btn, button {{ display:inline-flex; align-items:center; justify-content:center; gap:8px; border:0; border-radius:14px; background:var(--primary); color:#fff; padding:12px 18px; font-weight:700; cursor:pointer; }}
.btn:hover, button:hover {{ background:var(--primary-hover); }}
.btn.secondary {{ background:#fff; color:var(--primary); border:1px solid var(--border); }}
.btn.secondary:hover {{ background:var(--surface-2); }}
.flash {{ margin-bottom:18px; background:#f5f3ff; color:#4c1d95; border:1px solid #ddd6fe; padding:14px 16px; border-radius:14px; }}
.form-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); gap:16px; }}
.field {{ display:flex; flex-direction:column; gap:8px; }}
label {{ font-weight:700; font-size:14px; }}
input, select, textarea {{ width:100%; padding:12px 14px; border:1px solid var(--border); border-radius:14px; background:#fff; color:var(--text); font:inherit; }}
input:focus, select:focus, textarea:focus {{ outline:2px solid #c4b5fd; border-color:#a78bfa; }}
textarea {{ min-height:110px; resize:vertical; }}
.table-wrap {{ overflow:auto; padding:18px; }}
table {{ width:100%; border-collapse:collapse; }}
th, td {{ text-align:left; padding:12px 10px; border-bottom:1px solid #eee7ff; vertical-align:top; }}
th {{ color:#4c1d95; font-size:14px; }}
.badge {{ display:inline-block; padding:6px 10px; border-radius:999px; background:#f5f3ff; color:#5b21b6; font-weight:700; font-size:12px; }}
.inline {{ display:flex; flex-wrap:wrap; gap:12px; align-items:end; }}
.muted {{ color:var(--muted); }}
.login {{ min-height:100dvh; display:grid; place-items:center; padding:24px; }}
.login-card {{ width:min(500px,100%); background:#fff; border:1px solid var(--border); box-shadow:var(--shadow); border-radius:24px; padding:32px; }}
.kpi {{ font-size:30px; font-weight:800; color:var(--primary); margin-top:8px; }}
.checkbox-list {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; margin-top:8px; }}
.checkbox-item {{ border:1px solid var(--border); border-radius:14px; padding:12px; background:#fff; }}
.footer-note {{ margin-top:16px; color:var(--muted); font-size:14px; }}
@media (max-width: 720px) {{ .hero h1 {{ font-size:28px; }} .header-inner {{ align-items:flex-start; flex-direction:column; }} }}
</style>
</head>
<body>
<header class='header'><div class='header-inner'><div class='brand'>Ecom Secure Admin</div>{nav}</div></header>
<main class='main'>{flash_html}{body}</main>
</body></html>"""


def login_page(message=None):
    body = f"""
    <div class='login'>
      <section class='login-card'>
        <span class='badge'>Безопасная панель управления</span>
        <h1 style='margin:14px 0 10px;'>Вход в приложение</h1>
        <p class='muted' style='margin-bottom:22px;'>Пользователь приложения отделен от пользователя базы данных. Вход в интерфейс выполняется по отдельным учётным данным.</p>
        {f"<div class='flash'>{esc(message)}</div>" if message else ''}
        <form method='post' action='/login'>
          <div class='field'><label>Логин приложения</label><input name='username' maxlength='64' required></div>
          <div class='field'><label>Пароль приложения</label><input name='password' type='password' maxlength='128' required></div>
          <button type='submit' style='width:100%; margin-top:8px;'>Войти</button>
        </form>
      </section>
    </div>
    """
    return page_template("Вход", body)


def make_cookie(sid):
    cookie = cookies.SimpleCookie()
    cookie["sid"] = sid
    cookie["sid"]["httponly"] = True
    cookie["sid"]["path"] = "/"
    cookie["sid"]["samesite"] = "Strict"
    if COOKIE_SECURE:
        cookie["sid"]["secure"] = True
    return cookie.output(header="").strip()


def expire_cookie():
    cookie = cookies.SimpleCookie()
    cookie["sid"] = ""
    cookie["sid"]["httponly"] = True
    cookie["sid"]["path"] = "/"
    cookie["sid"]["max-age"] = 0
    cookie["sid"]["samesite"] = "Strict"
    return cookie.output(header="").strip()


def parse_post(handler):
    length = int(handler.headers.get("Content-Length", 0))
    raw = handler.rfile.read(length).decode("utf-8")
    return {k: v for k, v in parse_qs(raw, keep_blank_values=True).items()}


def first(form, key, default=""):
    return form.get(key, [default])[0]


def get_session(handler):
    jar = cookies.SimpleCookie()
    if "Cookie" in handler.headers:
        jar.load(handler.headers["Cookie"])
    sid = jar.get("sid")
    if not sid:
        return None, None
    return sid.value, SESSIONS.get(sid.value)


def require_auth(handler):
    sid, session = get_session(handler)
    if not session:
        handler.redirect("/login")
        return None, None
    return sid, session


def fetch_reference_data(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM categories ORDER BY name")
        categories = cur.fetchall()
        cur.execute("SELECT id, first_name || ' ' || last_name AS label FROM customers ORDER BY first_name, last_name")
        customers = cur.fetchall()
        cur.execute("SELECT id, name FROM products ORDER BY name")
        products = cur.fetchall()
        cur.execute("SELECT o.id, c.first_name || ' ' || c.last_name || ' / ' || to_char(o.order_date, 'YYYY-MM-DD HH24:MI') AS label FROM orders o JOIN customers c ON c.id=o.customer_id ORDER BY o.order_date DESC")
        orders = cur.fetchall()
    return {"categories": categories, "customers": customers, "products": products, "orders": orders}


def product_lookup_rows(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.name, c.name AS category_name, p.price, p.stock, p.created_at, p.updated_at, p.description
            FROM products p
            JOIN categories c ON c.id = p.category_id
            ORDER BY p.name
        """)
        return cur.fetchall()


def customer_lookup_rows(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, first_name || ' ' || last_name AS full_name, email, phone, address, created_at
            FROM customers
            ORDER BY first_name, last_name
        """)
        return cur.fetchall()


def order_lookup_rows(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT o.id, c.first_name || ' ' || c.last_name AS customer_name, o.order_date, o.total_amount, o.status
            FROM orders o
            JOIN customers c ON c.id = o.customer_id
            ORDER BY o.order_date DESC
        """)
        return cur.fetchall()


def order_item_lookup_rows(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT oi.id,
                   c.first_name || ' ' || c.last_name || ' / ' || to_char(o.order_date, 'YYYY-MM-DD') AS order_label,
                   p.name AS product_name,
                   oi.quantity,
                   oi.price
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            JOIN customers c ON c.id = o.customer_id
            JOIN products p ON p.id = oi.product_id
            ORDER BY oi.id DESC
        """)
        return cur.fetchall()


def rows_for_table(conn, table):
    if table == "categories":
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, description, created_at FROM categories ORDER BY name")
            return cur.fetchall()
    if table == "products":
        return product_lookup_rows(conn)
    if table == "customers":
        return customer_lookup_rows(conn)
    if table == "orders":
        return order_lookup_rows(conn)
    if table == "order_items":
        return order_item_lookup_rows(conn)
    return []


def title_for_row(table, row):
    cfg = TABLE_CONFIG[table]
    return str(row.get(cfg["title"], "Запись"))


def public_options(conn, table):
    rows = rows_for_table(conn, table)
    options = []
    for row in rows:
        options.append({"value": row["id"], "label": title_for_row(table, row)})
    return options


def record_by_id(conn, table, record_id):
    with conn.cursor() as cur:
        query = sql.SQL("SELECT * FROM {} WHERE id = %s").format(sql.Identifier(table))
        cur.execute(query, (record_id,))
        return cur.fetchone()


def recalc_order_total(conn, order_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE orders
            SET total_amount = (
                SELECT COALESCE(SUM(quantity * price), 0)
                FROM order_items
                WHERE order_id = %s
            )
            WHERE id = %s
            """,
            (order_id, order_id),
        )


def insert_one(conn, table, data):
    fields = TABLE_CONFIG[table]["fields"]
    filtered = {k: v for k, v in data.items() if k in fields and v is not None}
    if not filtered:
        raise ValueError("Нет данных для добавления")
    with conn.cursor() as cur:
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, filtered.keys())),
            sql.SQL(", ").join([sql.Placeholder()] * len(filtered)),
        )
        cur.execute(query, list(filtered.values()))
        new_id = cur.fetchone()["id"]
    if table == "orders":
        recalc_order_total(conn, new_id)
    if table == "order_items":
        recalc_order_total(conn, filtered["order_id"])
    return new_id


def update_one(conn, table, record_id, data):
    fields = TABLE_CONFIG[table]["fields"]
    filtered = {k: v for k, v in data.items() if k in fields and v is not None}
    if not filtered:
        raise ValueError("Нет данных для обновления")
    with conn.cursor() as cur:
        set_clause = sql.SQL(", ").join([
            sql.SQL("{} = %s").format(sql.Identifier(k)) for k in filtered.keys()
        ])
        query = sql.SQL("UPDATE {} SET {} WHERE id = %s").format(sql.Identifier(table), set_clause)
        cur.execute(query, list(filtered.values()) + [record_id])
        if cur.rowcount == 0:
            raise ValueError("Запись не найдена")
        if table == "products":
            cur.execute("UPDATE products SET updated_at = CURRENT_TIMESTAMP WHERE id = %s", (record_id,))
    if table == "orders":
        recalc_order_total(conn, record_id)
    if table == "order_items":
        with conn.cursor() as cur:
            cur.execute("SELECT order_id FROM order_items WHERE id = %s", (record_id,))
            row = cur.fetchone()
        if row:
            recalc_order_total(conn, row["order_id"])


def update_many(conn, table, ids, data):
    fields = TABLE_CONFIG[table]["fields"]
    filtered = {k: v for k, v in data.items() if k in fields and v is not None}
    if not filtered:
        raise ValueError("Нет данных для обновления")
    if not ids:
        raise ValueError("Не выбраны записи")
    in_clause = build_in_clause(ids)
    with conn.cursor() as cur:
        set_clause = sql.SQL(", ").join([
            sql.SQL("{} = %s").format(sql.Identifier(k)) for k in filtered.keys()
        ])
        query = sql.SQL("UPDATE {} SET {} WHERE id IN ({})").format(sql.Identifier(table), set_clause, in_clause)
        params = list(filtered.values()) + ids
        cur.execute(query, params)
        affected = cur.rowcount
        if table == "products":
            query2 = sql.SQL("UPDATE products SET updated_at = CURRENT_TIMESTAMP WHERE id IN ({})").format(in_clause)
            cur.execute(query2, ids)
    if table == "orders":
        for rid in ids:
            recalc_order_total(conn, rid)
    if table == "order_items":
        with conn.cursor() as cur:
            q = sql.SQL("SELECT DISTINCT order_id FROM order_items WHERE id IN ({})").format(in_clause)
            cur.execute(q, ids)
            order_ids = [r["order_id"] for r in cur.fetchall()]
        for oid in order_ids:
            recalc_order_total(conn, oid)
    return affected


def parse_entity_form(table, form):
    if table == "categories":
        return {
            "name": normalize_text(first(form, "name"), 100),
            "description": normalize_text(first(form, "description"), 2000),
        }
    if table == "products":
        return {
            "name": normalize_text(first(form, "name"), 255),
            "description": normalize_text(first(form, "description"), 2000),
            "category_id": normalize_int(first(form, "category_id"), 1),
            "price": normalize_decimal(first(form, "price"), 0.01),
            "stock": normalize_int(first(form, "stock"), 0),
        }
    if table == "customers":
        return {
            "first_name": normalize_text(first(form, "first_name"), 100),
            "last_name": normalize_text(first(form, "last_name"), 100),
            "email": normalize_email(first(form, "email")),
            "phone": normalize_text(first(form, "phone"), 20),
            "address": normalize_text(first(form, "address"), 1000),
        }
    if table == "orders":
        status = normalize_text(first(form, "status"), 20)
        if status not in STATUS_VALUES:
            raise ValueError("Некорректный статус")
        return {
            "customer_id": normalize_int(first(form, "customer_id"), 1),
            "status": status,
        }
    if table == "order_items":
        return {
            "order_id": normalize_int(first(form, "order_id"), 1),
            "product_id": normalize_int(first(form, "product_id"), 1),
            "quantity": normalize_int(first(form, "quantity"), 1),
            "price": normalize_decimal(first(form, "price"), 0.01),
        }
    raise ValueError("Недопустимая таблица")


def select_options_html(options, selected=None):
    parts = []
    for item in options:
        sel = " selected" if str(item["value"]) == str(selected) else ""
        parts.append(f"<option value='{esc(item['value'])}'{sel}>{esc(item['label'])}</option>")
    return "".join(parts)


def table_select(selected="categories"):
    opts = []
    for key, cfg in TABLE_CONFIG.items():
        sel = " selected" if key == selected else ""
        opts.append(f"<option value='{key}'{sel}>{esc(cfg['label'])}</option>")
    return "".join(opts)


def render_entity_form(conn, table, values=None, mode="add"):
    values = values or {}
    refs = fetch_reference_data(conn)
    if table == "categories":
        return f"""
        <div class='form-grid'>
          <div class='field'><label>Название</label><input name='name' maxlength='100' value='{esc(values.get('name',''))}' required></div>
          <div class='field'><label>Описание</label><textarea name='description'>{esc(values.get('description',''))}</textarea></div>
        </div>
        """
    if table == "products":
        return f"""
        <div class='form-grid'>
          <div class='field'><label>Название</label><input name='name' maxlength='255' value='{esc(values.get('name',''))}' required></div>
          <div class='field'><label>Категория</label><select name='category_id' required>{select_options_html([{'value':'','label':'Выберите категорию'}] + refs['categories'], values.get('category_id'))}</select></div>
          <div class='field'><label>Цена</label><input name='price' type='number' min='0.01' step='0.01' value='{esc(values.get('price',''))}' required></div>
          <div class='field'><label>Остаток</label><input name='stock' type='number' min='0' step='1' value='{esc(values.get('stock',''))}' required></div>
          <div class='field' style='grid-column:1/-1;'><label>Описание</label><textarea name='description'>{esc(values.get('description',''))}</textarea></div>
        </div>
        """
    if table == "customers":
        return f"""
        <div class='form-grid'>
          <div class='field'><label>Имя</label><input name='first_name' maxlength='100' value='{esc(values.get('first_name',''))}' required></div>
          <div class='field'><label>Фамилия</label><input name='last_name' maxlength='100' value='{esc(values.get('last_name',''))}' required></div>
          <div class='field'><label>Email</label><input name='email' type='email' maxlength='255' value='{esc(values.get('email',''))}' required></div>
          <div class='field'><label>Телефон</label><input name='phone' maxlength='20' value='{esc(values.get('phone',''))}'></div>
          <div class='field' style='grid-column:1/-1;'><label>Адрес</label><textarea name='address'>{esc(values.get('address',''))}</textarea></div>
        </div>
        """
    if table == "orders":
        customer_opts = [{"value":"","label":"Выберите клиента"}] + refs["customers"]
        status_opts = "".join([f"<option value='{s}'{' selected' if values.get('status') == s else ''}>{s}</option>" for s in STATUS_VALUES])
        return f"""
        <div class='form-grid'>
          <div class='field'><label>Клиент</label><select name='customer_id' required>{select_options_html(customer_opts, values.get('customer_id'))}</select></div>
          <div class='field'><label>Статус</label><select name='status'>{status_opts}</select></div>
        </div>
        """
    if table == "order_items":
        order_opts = [{"value":"","label":"Выберите заказ"}] + refs["orders"]
        product_opts = [{"value":"","label":"Выберите товар"}] + refs["products"]
        return f"""
        <div class='form-grid'>
          <div class='field'><label>Заказ</label><select name='order_id' required>{select_options_html(order_opts, values.get('order_id'))}</select></div>
          <div class='field'><label>Товар</label><select name='product_id' required>{select_options_html(product_opts, values.get('product_id'))}</select></div>
          <div class='field'><label>Количество</label><input name='quantity' type='number' min='1' step='1' value='{esc(values.get('quantity',''))}' required></div>
          <div class='field'><label>Цена позиции</label><input name='price' type='number' min='0.01' step='0.01' value='{esc(values.get('price',''))}' required></div>
        </div>
        """
    return ""


def render_table(conn, table, q=""):
    rows = rows_for_table(conn, table)
    cfg = TABLE_CONFIG[table]
    if q:
        needle = q.lower()
        filtered = []
        for row in rows:
            hay = " ".join([str(row.get(k, "")) for k in row.keys()]).lower()
            if needle in hay:
                filtered.append(row)
        rows = filtered
    headers = cfg["display"]
    thead = "".join([f"<th>{esc(h.replace('_',' ').title())}</th>" for h in headers]) + "<th>Действие</th>"
    trs = []
    for row in rows:
        cells = []
        for h in headers:
            val = row.get(h, "")
            if h == "status":
                val = f"<span class='badge'>{esc(val)}</span>"
            else:
                val = esc(val)
            cells.append(f"<td>{val}</td>")
        cells.append(f"<td><a class='btn secondary' href='/update?table={esc(table)}&record={row['id']}'>Изменить</a></td>")
        trs.append(f"<tr>{''.join(cells)}</tr>")
    body = "".join(trs) or f"<tr><td colspan='{len(headers)+1}' class='muted'>Нет данных</td></tr>"
    return f"<div class='table-wrap'><table><thead><tr>{thead}</tr></thead><tbody>{body}</tbody></table></div>"


def batch_add_page(conn, table='order_bundle', message=None):
    refs = fetch_reference_data(conn)
    customer_opts = select_options_html([{'value':'','label':'Выберите клиента'}] + refs['customers'])
    product_opts = select_options_html(refs['products'])
    category_opts = select_options_html(refs['categories'])
    body = f"""
    <section class='hero'>
      <span class='badge'>Пакетное добавление</span>
      <h1>Добавление в одну или несколько таблиц</h1>
      <p>Поддерживаются два безопасных сценария: создание заказа с несколькими позициями и создание категории с несколькими товарами. Внутренние идентификаторы пользователю не показываются там, где это не нужно.</p>
    </section>
    {f"<div class='flash'>{esc(message)}</div>" if message else ''}
    <div class='grid'>
      <section class='card'>
        <h3>Заказ + несколько позиций</h3>
        <form method='post' action='/batch-add-order'>
          <div class='field'><label>Клиент</label><select name='customer_id' required>{customer_opts}</select></div>
          <div class='field'><label>Статус</label><select name='status'>{''.join([f"<option value='{s}'>{s}</option>" for s in STATUS_VALUES])}</select></div>
          <div class='field'><label>Товары и количества</label>
            <div class='checkbox-list'>
              {''.join([f"<label class='checkbox-item'><input type='checkbox' name='product_id' value='{p['id']}'> {esc(p['name'])}</label>" for p in refs['products']])}
            </div>
          </div>
          <p class='footer-note'>После выбора товаров задайте количества в формате: <strong>идентификатор=количество</strong>, по одному на строку. Эти значения проходят строгую валидацию и не подставляются в SQL строкой.</p>
          <div class='field'><label>Количества</label><textarea name='quantities' placeholder='1=2\n3=1'></textarea></div>
          <button type='submit'>Создать заказ</button>
        </form>
      </section>
      <section class='card'>
        <h3>Категория + несколько товаров</h3>
        <form method='post' action='/batch-add-category-products'>
          <div class='field'><label>Новая категория</label><input name='category_name' required maxlength='100'></div>
          <div class='field'><label>Описание категории</label><textarea name='category_description'></textarea></div>
          <p class='footer-note'>Добавьте товары по одному на строку в формате: <strong>Название | Цена | Остаток | Описание</strong></p>
          <div class='field'><label>Товары</label><textarea name='products_blob' placeholder='Фиолетовая кружка | 990.00 | 12 | Керамика\nЧехол | 1490.00 | 7 | Защитный'></textarea></div>
          <button type='submit'>Создать категорию и товары</button>
        </form>
      </section>
    </div>
    """
    return page_template("Пакетное добавление", body, user=True)


def parse_quantities_blob(blob):
    result = {}
    for line in (blob or '').splitlines():
        line = line.strip()
        if not line:
            continue
        if '=' not in line:
            raise ValueError('Некорректный формат количества')
        left, right = line.split('=', 1)
        pid = normalize_int(left, 1)
        qty = normalize_int(right, 1)
        result[pid] = qty
    return result


def parse_products_blob(blob):
    rows = []
    for line in (blob or '').splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split('|')]
        if len(parts) < 3:
            raise ValueError('Каждая строка товара должна содержать минимум название, цену и остаток')
        name = normalize_text(parts[0], 255)
        price = normalize_decimal(parts[1], 0.01)
        stock = normalize_int(parts[2], 0)
        desc = normalize_text(parts[3], 2000) if len(parts) > 3 else None
        rows.append({'name': name, 'price': price, 'stock': stock, 'description': desc})
    if not rows:
        raise ValueError('Нет товаров для добавления')
    if len(rows) > BATCH_LIMIT:
        raise ValueError('Слишком много товаров')
    return rows


class AppHandler(BaseHTTPRequestHandler):
    def send_html(self, content, status=200, set_cookie=None, location=None):
        self.send_response(status)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        if set_cookie:
            self.send_header('Set-Cookie', set_cookie)
        if location:
            self.send_header('Location', location)
        self.end_headers()
        if content:
            self.wfile.write(content.encode('utf-8'))

    def redirect(self, location, set_cookie=None):
        self.send_html('', status=302, set_cookie=set_cookie, location=location)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == '/':
            _, session = get_session(self)
            self.redirect('/home' if session else '/login')
            return

        if path == '/login':
            self.send_html(login_page())
            return

        if path == '/logout':
            sid, _ = get_session(self)
            if sid and sid in SESSIONS:
                del SESSIONS[sid]
            self.redirect('/login', set_cookie=expire_cookie())
            return

        _, session = require_auth(self)
        if not session:
            return

        try:
            with get_conn() as conn:
                if path == '/home':
                    with conn.cursor() as cur:
                        cur.execute('SELECT COUNT(*) AS c FROM products')
                        products = cur.fetchone()['c']
                        cur.execute('SELECT COUNT(*) AS c FROM customers')
                        customers = cur.fetchone()['c']
                        cur.execute('SELECT COUNT(*) AS c FROM orders')
                        orders = cur.fetchone()['c']
                    body = f"""
                    <section class='hero'>
                      <span class='badge'>Внутренний интерфейс</span>
                      <h1>Безопасное управление e-commerce данными</h1>
                      <p>Интерфейс отделён от PostgreSQL-учётной записи: пользователь приложения проходит вход по отдельному логину приложения, а само приложение работает в БД от выделенного служебного пользователя с ограниченными правами.</p>
                    </section>
                    <section class='grid'>
                      <article class='card'><h3>Товары</h3><div class='kpi'>{products}</div><p>Просмотр каталога, безопасное добавление и редактирование.</p><a class='btn' href='/tables?table=products'>Открыть</a></article>
                      <article class='card'><h3>Клиенты</h3><div class='kpi'>{customers}</div><p>Управление клиентами без показа внутренних идентификаторов в списках.</p><a class='btn' href='/tables?table=customers'>Открыть</a></article>
                      <article class='card'><h3>Заказы</h3><div class='kpi'>{orders}</div><p>Создание заказов и пакетная работа с позициями.</p><a class='btn' href='/tables?table=orders'>Открыть</a></article>
                    </section>
                    """
                    self.send_html(page_template('Главная', body, user=session['username']))
                    return

                if path == '/tables':
                    table = first(query, 'table', 'categories')
                    if table not in TABLE_CONFIG:
                        raise ValueError('Недопустимая таблица')
                    q = normalize_text(first(query, 'q', ''), 100) or ''
                    body = f"""
                    <section class='hero'>
                      <span class='badge'>Просмотр данных</span>
                      <h1>{esc(TABLE_CONFIG[table]['label'])}</h1>
                      <p>Списки не показывают технические идентификаторы, когда они не нужны пользователю. Для навигации используются понятные названия, email, составные подписи и даты.</p>
                    </section>
                    <section class='card' style='margin-bottom:18px;'>
                      <form method='get' action='/tables' class='inline'>
                        <div class='field'><label>Таблица</label><select name='table'>{table_select(table)}</select></div>
                        <div class='field'><label>Поиск</label><input name='q' value='{esc(q)}' placeholder='Например, ноутбук или delivered'></div>
                        <button type='submit'>Показать</button>
                      </form>
                    </section>
                    {render_table(conn, table, q)}
                    """
                    self.send_html(page_template('Таблицы', body, user=session['username']))
                    return

                if path == '/add':
                    table = first(query, 'table', 'categories')
                    if table not in TABLE_CONFIG:
                        raise ValueError('Недопустимая таблица')
                    body = f"""
                    <section class='hero'><span class='badge'>Добавление</span><h1>Новая запись</h1><p>Выберите сущность и заполните форму. Все значения проходят серверную валидацию и передаются в SQL только параметрами.</p></section>
                    <section class='card'>
                      <form method='get' action='/add' class='inline' style='margin-bottom:18px;'>
                        <div class='field'><label>Сущность</label><select name='table'>{table_select(table)}</select></div>
                        <button type='submit' class='secondary btn'>Переключить форму</button>
                      </form>
                      <form method='post' action='/add'>
                        <input type='hidden' name='table' value='{esc(table)}'>
                        {render_entity_form(conn, table)}
                        <button type='submit' style='margin-top:18px;'>Сохранить</button>
                      </form>
                    </section>
                    """
                    self.send_html(page_template('Добавление', body, user=session['username']))
                    return

                if path == '/batch-add':
                    self.send_html(batch_add_page(conn), status=200)
                    return

                if path == '/update':
                    table = first(query, 'table', 'categories')
                    if table not in TABLE_CONFIG:
                        raise ValueError('Недопустимая таблица')
                    record_id = normalize_int(first(query, 'record', ''), 1)
                    form_html = ''
                    if record_id:
                        record = record_by_id(conn, table, record_id)
                        if not record:
                            raise ValueError('Запись не найдена')
                        form_html = f"""
                        <form method='post' action='/update'>
                          <input type='hidden' name='table' value='{esc(table)}'>
                          <input type='hidden' name='record_id' value='{record_id}'>
                          {render_entity_form(conn, table, record, mode='update')}
                          <button type='submit' style='margin-top:18px;'>Сохранить изменения</button>
                        </form>
                        """
                    options = public_options(conn, table)
                    body = f"""
                    <section class='hero'><span class='badge'>Обновление одной записи</span><h1>Редактирование</h1><p>Идентификаторы не выводятся в интерфейсе списка. Пользователь выбирает запись по понятному названию, а внутренний id передается скрыто и обрабатывается на сервере.</p></section>
                    <section class='card' style='margin-bottom:18px;'>
                      <form method='get' action='/update' class='inline'>
                        <div class='field'><label>Сущность</label><select name='table'>{table_select(table)}</select></div>
                        <div class='field'><label>Запись</label><select name='record'>{select_options_html([{'value':'','label':'Выберите запись'}] + options, record_id)}</select></div>
                        <button type='submit'>Открыть</button>
                      </form>
                    </section>
                    <section class='card'>{form_html or '<p class="muted">Выберите запись для редактирования.</p>'}</section>
                    """
                    self.send_html(page_template('Обновление', body, user=session['username']))
                    return

                if path == '/batch-update':
                    table = first(query, 'table', 'products')
                    if table not in TABLE_CONFIG:
                        raise ValueError('Недопустимая таблица')
                    options = public_options(conn, table)
                    body = f"""
                    <section class='hero'><span class='badge'>Массовое обновление</span><h1>Обновление нескольких записей</h1><p>Для группы однотипных значений используется безопасная передача массива идентификаторов: значения валидируются, а затем подставляются в SQL только через динамически сгенерированные плейсхолдеры.</p></section>
                    <section class='card'>
                      <form method='get' action='/batch-update' class='inline' style='margin-bottom:18px;'>
                        <div class='field'><label>Сущность</label><select name='table'>{table_select(table)}</select></div>
                        <button type='submit' class='btn secondary'>Показать записи</button>
                      </form>
                      <form method='post' action='/batch-update'>
                        <input type='hidden' name='table' value='{esc(table)}'>
                        <div class='field'><label>Выберите записи</label><div class='checkbox-list'>{''.join([f"<label class='checkbox-item'><input type='checkbox' name='record_id' value='{o['value']}'> {esc(o['label'])}</label>" for o in options])}</div></div>
                        <div style='margin-top:18px;'>{render_entity_form(conn, table)}</div>
                        <button type='submit' style='margin-top:18px;'>Обновить выбранные записи</button>
                      </form>
                    </section>
                    """
                    self.send_html(page_template('Массовое обновление', body, user=session['username']))
                    return

                self.send_html(page_template('404', '<section class="card"><h1>Страница не найдена</h1></section>', user=session['username']), status=404)
        except Exception as e:
            self.send_html(page_template('Ошибка', f"<section class='card'><h1>Ошибка</h1><p>{esc(str(e))}</p></section>", user=session['username']), status=400)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        form = parse_post(self)

        if path == '/login':
            username = normalize_text(first(form, 'username'), 64) or ''
            password = first(form, 'password')
            if app_user_valid(username, password):
                sid = new_session(username)
                self.redirect('/home', set_cookie=make_cookie(sid))
            else:
                logging.warning('failed_app_login username=%s ip=%s', username, self.client_address[0])
                self.send_html(login_page('Неверный логин или пароль приложения'), status=401)
            return

        _, session = require_auth(self)
        if not session:
            return

        try:
            with get_conn() as conn:
                if path == '/add':
                    table = first(form, 'table')
                    if table not in TABLE_CONFIG:
                        raise ValueError('Недопустимая таблица')
                    data = parse_entity_form(table, form)
                    insert_one(conn, table, data)
                    conn.commit()
                    self.redirect('/tables?' + urlencode({'table': table}))
                    return

                if path == '/update':
                    table = first(form, 'table')
                    record_id = normalize_int(first(form, 'record_id'), 1)
                    if table not in TABLE_CONFIG or not record_id:
                        raise ValueError('Некорректные параметры')
                    data = parse_entity_form(table, form)
                    update_one(conn, table, record_id, data)
                    conn.commit()
                    self.redirect('/tables?' + urlencode({'table': table}))
                    return

                if path == '/batch-update':
                    table = first(form, 'table')
                    if table not in TABLE_CONFIG:
                        raise ValueError('Недопустимая таблица')
                    ids = parse_id_list(form.get('record_id', []))
                    data = parse_entity_form(table, form)
                    update_many(conn, table, ids, data)
                    conn.commit()
                    self.redirect('/tables?' + urlencode({'table': table}))
                    return

                if path == '/batch-add-order':
                    customer_id = normalize_int(first(form, 'customer_id'), 1)
                    status = normalize_text(first(form, 'status'), 20)
                    if status not in STATUS_VALUES:
                        raise ValueError('Некорректный статус')
                    selected_product_ids = parse_id_list(form.get('product_id', []))
                    quantities = parse_quantities_blob(first(form, 'quantities'))
                    if not selected_product_ids:
                        raise ValueError('Не выбраны товары')
                    with conn.cursor() as cur:
                        cur.execute('INSERT INTO orders (customer_id, status) VALUES (%s, %s) RETURNING id', (customer_id, status))
                        order_id = cur.fetchone()['id']
                        placeholders = build_in_clause(selected_product_ids)
                        q = sql.SQL('SELECT id, price FROM products WHERE id IN ({})').format(placeholders)
                        cur.execute(q, selected_product_ids)
                        price_map = {r['id']: r['price'] for r in cur.fetchall()}
                        missing = [pid for pid in selected_product_ids if pid not in price_map]
                        if missing:
                            raise ValueError('Один или несколько товаров не найдены')
                        items = []
                        for pid in selected_product_ids:
                            qty = quantities.get(pid, 1)
                            items.append((order_id, pid, qty, price_map[pid]))
                        cur.executemany('INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s)', items)
                    recalc_order_total(conn, order_id)
                    conn.commit()
                    self.redirect('/tables?table=orders')
                    return

                if path == '/batch-add-category-products':
                    category_name = normalize_text(first(form, 'category_name'), 100)
                    category_description = normalize_text(first(form, 'category_description'), 2000)
                    products = parse_products_blob(first(form, 'products_blob'))
                    with conn.cursor() as cur:
                        cur.execute('INSERT INTO categories (name, description) VALUES (%s, %s) RETURNING id', (category_name, category_description))
                        category_id = cur.fetchone()['id']
                        rows = [(p['name'], p['description'], category_id, p['price'], p['stock']) for p in products]
                        cur.executemany('INSERT INTO products (name, description, category_id, price, stock) VALUES (%s, %s, %s, %s, %s)', rows)
                    conn.commit()
                    self.redirect('/tables?table=products')
                    return

                self.send_html(page_template('404', '<section class="card"><h1>Страница не найдена</h1></section>', user=session['username']), status=404)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            body = f"<section class='card'><h1>Ошибка операции</h1><p>{esc(str(e))}</p></section>"
            self.send_html(page_template('Ошибка', body, user=session['username']), status=400)


if __name__ == '__main__':
    server = HTTPServer((HOST, PORT), AppHandler)
    print(f'Server started on http://{HOST}:{PORT}')
    server.serve_forever()
##
