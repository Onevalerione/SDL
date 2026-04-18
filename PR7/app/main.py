import os
import json
import logging
import secrets
from decimal import Decimal, InvalidOperation
from functools import wraps
from urllib.parse import parse_qs, quote
from http import cookies
from http.server import BaseHTTPRequestHandler, HTTPServer

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "5000"))
APP_USER = os.getenv("APP_USER", "admin")
APP_PASSWORD = os.getenv("APP_PASSWORD", "admin123")
SESSION_COOKIE = "ecom_session"
LOG_DIR = os.getenv("LOG_DIR", "/app/logs")
LOG_FILE = os.getenv("LOG_FILE", os.path.join(LOG_DIR, "app.log"))
AUTH_LOG_FILE = os.getenv("AUTH_LOG_FILE", os.path.join(LOG_DIR, "auth.log"))

os.makedirs(LOG_DIR, exist_ok=True)

formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger = logging.getLogger("ecom_app")
logger.setLevel(logging.INFO)
logger.handlers = []
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

security_logger = logging.getLogger("ecom_auth")
security_logger.setLevel(logging.INFO)
security_logger.handlers = []
auth_handler = logging.FileHandler(AUTH_LOG_FILE, encoding="utf-8")
auth_handler.setFormatter(formatter)
security_logger.addHandler(auth_handler)
security_logger.addHandler(stream_handler)

SESSIONS = {}
VALID_TABLES = {"categories", "products", "customers", "orders", "order_items"}
DISPLAY_NAMES = {
    "categories": "Категории",
    "products": "Товары",
    "customers": "Клиенты",
    "orders": "Заказы",
    "order_items": "Позиции заказов",
}
SELECT_QUERIES = {
    "categories": "SELECT name, description, created_at FROM categories ORDER BY name",
    "products": """
        SELECT p.name, COALESCE(c.name, '—') AS category, p.description, p.price, p.stock, p.created_at
        FROM products p
        LEFT JOIN categories c ON c.id = p.category_id
        ORDER BY p.name
    """,
    "customers": "SELECT first_name, last_name, email, phone, address, created_at FROM customers ORDER BY last_name, first_name",
    "orders": """
        SELECT c.email, o.order_date, o.total_amount, o.status
        FROM orders o
        JOIN customers c ON c.id = o.customer_id
        ORDER BY o.order_date DESC, o.id DESC
    """,
    "order_items": """
        SELECT c.email, p.name, oi.quantity, oi.price
        FROM order_items oi
        JOIN orders o ON o.id = oi.order_id
        JOIN customers c ON c.id = o.customer_id
        JOIN products p ON p.id = oi.product_id
        ORDER BY o.id DESC, p.name
    """,
}


def escape_html(value):
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "ecommerce"),
        user=os.getenv("DB_USER", "appuser"),
        password=os.getenv("DB_PASSWORD", "securepassword123"),
    )
    conn.autocommit = False
    return conn


def html_page(title, body, username=None):
    auth_block = ""
    if username:
        auth_block = f'<div class="userbox">Вошли как <b>{escape_html(username)}</b> · <a href="/logout">Выйти</a></div>'
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape_html(title)}</title>
  <style>
    :root {{ --bg:#ffffff; --surface:#f6f2ff; --line:#e5d9ff; --text:#24143a; --muted:#6d5a8a; --primary:#6f3ff5; --primary-hover:#5d31db; --danger:#b42318; --success:#0f9d58; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:Arial, sans-serif; background:var(--bg); color:var(--text); }}
    .wrap {{ max-width:1180px; margin:0 auto; padding:24px; }}
    .top {{ display:flex; justify-content:space-between; align-items:center; gap:16px; margin-bottom:24px; }}
    .brand {{ font-size:28px; font-weight:700; }}
    .userbox {{ color:var(--muted); font-size:14px; }}
    .grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:16px; margin:20px 0 28px; }}
    .card {{ background:#fff; border:1px solid var(--line); border-radius:18px; padding:18px; box-shadow:0 8px 24px rgba(111,63,245,.08); }}
    .card h3 {{ margin:0 0 8px; }}
    .nav a, .btn {{ display:inline-block; text-decoration:none; background:var(--primary); color:#fff; padding:10px 16px; border-radius:12px; border:none; cursor:pointer; font-weight:700; }}
    .nav a:hover, .btn:hover {{ background:var(--primary-hover); }}
    .section {{ margin:24px 0; }}
    .section h2 {{ margin:0 0 12px; }}
    table {{ width:100%; border-collapse:collapse; background:#fff; border-radius:18px; overflow:hidden; border:1px solid var(--line); }}
    th, td {{ padding:12px; border-bottom:1px solid #f0eafe; text-align:left; vertical-align:top; }}
    th {{ background:var(--surface); }}
    form {{ background:#fff; border:1px solid var(--line); border-radius:18px; padding:18px; }}
    label {{ display:block; font-size:14px; color:var(--muted); margin:12px 0 6px; font-weight:700; }}
    input, select, textarea {{ width:100%; padding:12px; border:1px solid #d7c9ff; border-radius:12px; font-size:15px; }}
    textarea {{ min-height:100px; resize:vertical; }}
    .msg {{ padding:14px 16px; border-radius:14px; margin:12px 0; }}
    .msg.ok {{ background:#ecfdf3; color:#067647; border:1px solid #abefc6; }}
    .msg.err {{ background:#fef3f2; color:#b42318; border:1px solid #fecdca; }}
    .small {{ color:var(--muted); font-size:13px; }}
    .actions {{ display:flex; flex-wrap:wrap; gap:10px; margin-top:16px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div class="brand">E-Commerce Admin</div>
      {auth_block}
    </div>
    {body}
  </div>
</body>
</html>"""


def parse_cookies(header):
    c = cookies.SimpleCookie()
    if header:
        c.load(header)
    return {k: morsel.value for k, morsel in c.items()}


def require_auth(handler_method):
    @wraps(handler_method)
    def wrapper(self, *args, **kwargs):
        sid = parse_cookies(self.headers.get("Cookie", "")).get(SESSION_COOKIE)
        username = SESSIONS.get(sid)
        if not username:
            self.redirect("/login")
            return
        self.current_user = username
        return handler_method(self, *args, **kwargs)
    return wrapper


def normalize_value(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        return value
    text = str(value).strip()
    if text == "":
        return None
    if text.isdigit():
        return int(text)
    try:
        return Decimal(text)
    except InvalidOperation:
        return text


def get_valid_columns(cur, table, include_id=False):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s", (table,))
    cols = {row[0] for row in cur.fetchall()}
    if not include_id:
        cols.discard("id")
    return cols


def insert_single(table, payload):
    if table not in {"categories", "products", "customers"}:
        raise ValueError("Недопустимая таблица для одиночного добавления")
    with get_db_connection() as conn, conn.cursor() as cur:
        valid_columns = get_valid_columns(cur, table)
        filtered = {k: normalize_value(v) for k, v in payload.items() if k in valid_columns}
        if not filtered:
            raise ValueError("Нет допустимых полей")

        required_fields = {
            "categories": ["name"],
            "products": ["name", "category_id", "price"],
            "customers": ["first_name", "last_name", "email"],
        }
        missing = [field for field in required_fields.get(table, []) if filtered.get(field) is None]
        if missing:
            raise ValueError(f"Не заполнены обязательные поля: {', '.join(missing)}")

        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, filtered.keys())),
            sql.SQL(", ").join(sql.Placeholder() for _ in filtered),
        )
        logger.info("SQL INSERT table=%s columns=%s values_count=%s", table, list(filtered.keys()), len(filtered))
        cur.execute(query, list(filtered.values()))
        conn.commit()


def bulk_add_category_products(payload):
    category = payload.get("category", {})
    products = payload.get("products", [])
    if not category or not products:
        raise ValueError("Нужны category и products")
    with get_db_connection() as conn, conn.cursor() as cur:
        category_cols = get_valid_columns(cur, "categories")
        filtered_category = {k: normalize_value(v) for k, v in category.items() if k in category_cols}
        if not filtered_category:
            raise ValueError("Нет полей категории")
        q1 = sql.SQL("INSERT INTO categories ({}) VALUES ({}) RETURNING id").format(
            sql.SQL(", ").join(map(sql.Identifier, filtered_category.keys())),
            sql.SQL(", ").join(sql.Placeholder() for _ in filtered_category),
        )
        cur.execute(q1, list(filtered_category.values()))
        category_id = cur.fetchone()[0]
        product_cols = get_valid_columns(cur, "products")
        logger.info("Создана категория category_id=%s, товаров к добавлению=%s", category_id, len(products))
        for idx, item in enumerate(products, start=1):
            item = dict(item)
            item["category_id"] = category_id
            filtered_product = {k: normalize_value(v) for k, v in item.items() if k in product_cols}
            q2 = sql.SQL("INSERT INTO products ({}) VALUES ({})").format(
                sql.SQL(", ").join(map(sql.Identifier, filtered_product.keys())),
                sql.SQL(", ").join(sql.Placeholder() for _ in filtered_product),
            )
            logger.info("Добавление товара %s/%s в category_id=%s columns=%s", idx, len(products), category_id, list(filtered_product.keys()))
            cur.execute(q2, list(filtered_product.values()))
        conn.commit()


def bulk_add_order_items(payload):
    order_data = payload.get("order", {})
    items = payload.get("items", [])
    if not order_data or not items:
        raise ValueError("Нужны order и items")
    with get_db_connection() as conn, conn.cursor() as cur:
        order_cols = get_valid_columns(cur, "orders")
        filtered_order = {k: normalize_value(v) for k, v in order_data.items() if k in order_cols}
        if not filtered_order:
            raise ValueError("Нет полей заказа")
        q1 = sql.SQL("INSERT INTO orders ({}) VALUES ({}) RETURNING id").format(
            sql.SQL(", ").join(map(sql.Identifier, filtered_order.keys())),
            sql.SQL(", ").join(sql.Placeholder() for _ in filtered_order),
        )
        cur.execute(q1, list(filtered_order.values()))
        order_id = cur.fetchone()[0]
        total = Decimal("0")
        item_cols = get_valid_columns(cur, "order_items")
        logger.info("Создан заказ order_id=%s, позиций к добавлению=%s", order_id, len(items))
        for idx, item in enumerate(items, start=1):
            item = dict(item)
            item["order_id"] = order_id
            filtered_item = {k: normalize_value(v) for k, v in item.items() if k in item_cols}
            q2 = sql.SQL("INSERT INTO order_items ({}) VALUES ({})").format(
                sql.SQL(", ").join(map(sql.Identifier, filtered_item.keys())),
                sql.SQL(", ").join(sql.Placeholder() for _ in filtered_item),
            )
            cur.execute(q2, list(filtered_item.values()))
            subtotal = Decimal(str(filtered_item.get("quantity", 0))) * Decimal(str(filtered_item.get("price", 0)))
            total += subtotal
            logger.info("Добавлена позиция %s/%s order_id=%s subtotal=%s", idx, len(items), order_id, subtotal)
        cur.execute("UPDATE orders SET total_amount=%s WHERE id=%s", (total, order_id))
        logger.info("Пересчитана сумма заказа order_id=%s total=%s", order_id, total)
        conn.commit()


def update_single(table, match_value, payload):
    if table not in VALID_TABLES:
        raise ValueError("Недопустимая таблица")

    lookup_columns = {
        "categories": "name",
        "products": "name",
        "customers": "email",
        "orders": "id",
        "order_items": "id",
    }
    lookup_column = lookup_columns.get(table)
    if not lookup_column:
        raise ValueError("Для таблицы не настроен поиск записи")

    with get_db_connection() as conn, conn.cursor() as cur:
        valid_columns = get_valid_columns(cur, table)
        filtered = {k: normalize_value(v) for k, v in payload.items() if k in valid_columns}
        if not filtered:
            raise ValueError("Нет допустимых полей")
        set_clause = sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(k)) for k in filtered.keys())
        query = sql.SQL("UPDATE {} SET {} WHERE {} = %s").format(
            sql.Identifier(table),
            set_clause,
            sql.Identifier(lookup_column),
        )
        normalized_match_value = normalize_value(match_value)
        params = list(filtered.values()) + [normalized_match_value]
        logger.info(
            "SQL UPDATE ONE table=%s lookup_column=%s match_value=%s columns=%s",
            table,
            lookup_column,
            normalized_match_value,
            list(filtered.keys()),
        )
        cur.execute(query, params)
        if cur.rowcount == 0:
            raise ValueError("Запись не найдена")
        conn.commit()


def bulk_update(table, filter_column, filter_values, payload):
    if table not in VALID_TABLES:
        raise ValueError("Недопустимая таблица")
    if not filter_values:
        raise ValueError("Пустой список значений фильтра")
    with get_db_connection() as conn, conn.cursor() as cur:
        valid_columns = get_valid_columns(cur, table, include_id=True)
        if filter_column not in valid_columns:
            raise ValueError("Недопустимая колонка фильтра")
        filtered = {k: normalize_value(v) for k, v in payload.items() if k in valid_columns and k != "id"}
        if not filtered:
            raise ValueError("Нет допустимых полей для обновления")
        normalized_filters = [normalize_value(v) for v in filter_values]
        set_clause = sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(k)) for k in filtered.keys())
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in normalized_filters)
        query = sql.SQL("UPDATE {} SET {} WHERE {} IN ({})").format(
            sql.Identifier(table),
            set_clause,
            sql.Identifier(filter_column),
            placeholders,
        )
        params = list(filtered.values()) + normalized_filters
        logger.info("SQL UPDATE BULK table=%s filter_column=%s filters_count=%s columns=%s", table, filter_column, len(normalized_filters), list(filtered.keys()))
        cur.execute(query, params)
        logger.info("Результат пакетного обновления table=%s updated_rows=%s", table, cur.rowcount)
        if cur.rowcount == 0:
            raise ValueError("Ни одна запись не обновлена")
        conn.commit()


class AppHandler(BaseHTTPRequestHandler):
    current_user = None

    def log_message(self, fmt, *args):
        logger.info("HTTP %s - %s", self.address_string(), fmt % args)

    def send_html(self, html, status=200, headers=None):
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        if headers:
            for key, val in headers.items():
                self.send_header(key, val)
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

    def redirect(self, location, headers=None):
        self.send_response(302)
        self.send_header("Location", location)
        if headers:
            for key, val in headers.items():
                self.send_header(key, val)
        self.end_headers()

    def parse_post_data(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode("utf-8") if length else ""
        return {k: v[0] if len(v) == 1 else v for k, v in parse_qs(raw, keep_blank_values=True).items()}

    def get_message(self):
        params = parse_qs(self.path.split("?", 1)[1] if "?" in self.path else "")
        ok = params.get("ok", [""])[0]
        err = params.get("err", [""])[0]
        parts = []
        if ok:
            parts.append(f'<div class="msg ok">{escape_html(ok)}</div>')
        if err:
            parts.append(f'<div class="msg err">{escape_html(err)}</div>')
        return "".join(parts)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        routes = {
            "/": self.handle_home,
            "/login": self.handle_login_get,
            "/logout": self.handle_logout,
            "/view": self.handle_view,
            "/add": self.handle_add_get,
            "/bulk-add": self.handle_bulk_add_get,
            "/update": self.handle_update_get,
            "/bulk-update": self.handle_bulk_update_get,
        }
        handler = routes.get(path)
        if handler:
            handler()
        else:
            self.send_html(html_page("404", "<div class='msg err'>Страница не найдена</div>"), 404)

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        routes = {
            "/login": self.handle_login_post,
            "/add": self.handle_add_post,
            "/bulk-add": self.handle_bulk_add_post,
            "/update": self.handle_update_post,
            "/bulk-update": self.handle_bulk_update_post,
        }
        handler = routes.get(path)
        if handler:
            handler()
        else:
            self.send_html(html_page("405", "<div class='msg err'>Метод не поддерживается</div>"), 405)

    def handle_login_get(self):
        body = f"""
        <div class='section'>
          <h2>Вход в приложение</h2>
          {self.get_message()}
          <form method='post' action='/login'>
            <label>Логин приложения</label>
            <input name='username' required>
            <label>Пароль приложения</label>
            <input type='password' name='password' required>
            <div class='actions'><button class='btn' type='submit'>Войти</button></div>
            <p class='small'>Пользователь приложения не совпадает с пользователем базы данных.</p>
          </form>
        </div>
        """
        self.send_html(html_page("Вход", body))

    def handle_login_post(self):
        data = self.parse_post_data()
        username = data.get("username", "")
        password = data.get("password", "")
        logger.info("Попытка входа: user=%s ip=%s ua=%s", username, self.client_address[0], self.headers.get("User-Agent", "-"))
        if username == APP_USER and password == APP_PASSWORD:
            sid = secrets.token_hex(24)
            SESSIONS[sid] = username
            logger.info("Успешный вход: user=%s ip=%s", username, self.client_address[0])
            self.redirect("/", headers={"Set-Cookie": f"{SESSION_COOKIE}={sid}; HttpOnly; Path=/; SameSite=Lax"})
        else:
            security_logger.warning("Неудачный вход: user=%s ip=%s ua=%s", username, self.client_address[0], self.headers.get("User-Agent", "-"))
            self.redirect("/login?err=" + quote("Неверный логин или пароль"))

    @require_auth
    def handle_logout(self):
        sid = parse_cookies(self.headers.get("Cookie", "")).get(SESSION_COOKIE)
        if sid in SESSIONS:
            logger.info("Выход: user=%s ip=%s", SESSIONS.get(sid), self.client_address[0])
            del SESSIONS[sid]
        self.redirect("/login", headers={"Set-Cookie": f"{SESSION_COOKIE}=deleted; expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/"})

    @require_auth
    def handle_home(self):
        cards = "".join(
            f"<div class='card'><h3>{name}</h3><div class='actions nav'><a href='/view?table={table}'>Просмотр</a></div></div>"
            for table, name in DISPLAY_NAMES.items()
        )
        body = f"""
        {self.get_message()}
        <div class='actions nav'>
          <a href='/add'>Добавить запись</a>
          <a href='/bulk-add'>Массовое добавление</a>
          <a href='/update'>Обновить запись</a>
          <a href='/bulk-update'>Пакетное обновление</a>
        </div>
        <div class='grid'>{cards}</div>
        """
        self.send_html(html_page("Главная", body, self.current_user))

    @require_auth
    def handle_view(self):
        params = parse_qs(self.path.split("?", 1)[1] if "?" in self.path else "")
        table = params.get("table", ["products"])[0]
        if table not in VALID_TABLES:
            self.redirect("/?err=" + quote("Недопустимая таблица"))
            return
        logger.info("Просмотр таблицы: table=%s user=%s ip=%s", table, self.current_user, self.client_address[0])
        try:
            with get_db_connection() as conn, conn.cursor() as cur:
                cur.execute(SELECT_QUERIES[table])
                rows = cur.fetchall()
                headers = [d[0] for d in cur.description]
            head_html = "".join(f"<th>{escape_html(h)}</th>" for h in headers)
            rows_html = "".join("<tr>" + "".join(f"<td>{escape_html(v)}</td>" for v in row) + "</tr>" for row in rows)
            if not rows_html:
                rows_html = f"<tr><td colspan='{len(headers)}'>Нет данных</td></tr>"
            body = f"""
            <div class='actions nav'><a href='/'>Назад</a></div>
            <div class='section'>
              <h2>{escape_html(DISPLAY_NAMES[table])}</h2>
              <table><thead><tr>{head_html}</tr></thead><tbody>{rows_html}</tbody></table>
            </div>
            """
            self.send_html(html_page(DISPLAY_NAMES[table], body, self.current_user))
        except Exception as e:
            logger.exception("Ошибка просмотра таблицы: table=%s error=%s", table, e)
            self.redirect("/?err=" + quote("Ошибка получения данных"))

    @require_auth
    def handle_add_get(self):
        body = f"""
        <div class='actions nav'><a href='/'>Назад</a></div>
        {self.get_message()}
        <div class='section'>
          <h2>Добавление одной записи</h2>
          <form method='post' action='/add'>
            <label>Таблица</label>
            <select name='table'>
              <option value='categories'>Категории</option>
              <option value='products'>Товары</option>
              <option value='customers'>Клиенты</option>
            </select>
            <label>Данные JSON</label>
            <textarea name='payload' placeholder='{{&quot;name&quot;:&quot;Ноутбуки&quot;,&quot;description&quot;:&quot;Раздел&quot;}}' required></textarea>
            <div class='actions'><button class='btn' type='submit'>Добавить</button></div>
          </form>
        </div>
        """
        self.send_html(html_page("Добавление", body, self.current_user))

    @require_auth
    def handle_add_post(self):
        data = self.parse_post_data()
        table = data.get("table", "")
        logger.info("Добавление записи: table=%s user=%s ip=%s", table, self.current_user, self.client_address[0])
        try:
            payload = json.loads(data.get("payload", "{}"))
            insert_single(table, payload)
            self.redirect("/?ok=" + quote("Запись успешно добавлена"))
        except Exception as e:
            logger.exception("Ошибка добавления записи: table=%s error=%s", table, e)
            self.redirect("/add?err=" + quote("Не удалось добавить запись"))

    @require_auth
    def handle_bulk_add_get(self):
        body = f"""
        <div class='actions nav'><a href='/'>Назад</a></div>
        {self.get_message()}
        <div class='section'>
          <h2>Массовое добавление</h2>
          <form method='post' action='/bulk-add'>
            <label>Сценарий</label>
            <select name='mode'>
              <option value='category_products'>Категория + несколько товаров</option>
              <option value='order_items'>Заказ + несколько позиций</option>
            </select>
            <label>Данные JSON</label>
            <textarea name='payload' required placeholder='{{&quot;category&quot;:{{&quot;name&quot;:&quot;Игры&quot;,&quot;description&quot;:&quot;...&quot;}},&quot;products&quot;:[{{&quot;name&quot;:&quot;Геймпад&quot;,&quot;description&quot;:&quot;...&quot;,&quot;price&quot;:&quot;99.90&quot;,&quot;stock&quot;:&quot;8&quot;}}]}}'></textarea>
            <div class='actions'><button class='btn' type='submit'>Выполнить массовое добавление</button></div>
          </form>
        </div>
        """
        self.send_html(html_page("Массовое добавление", body, self.current_user))

    @require_auth
    def handle_bulk_add_post(self):
        form = self.parse_post_data()
        mode = form.get("mode", "")
        logger.info("Массовое добавление: mode=%s user=%s ip=%s", mode, self.current_user, self.client_address[0])
        try:
            payload = json.loads(form.get("payload", "{}"))
            if mode == "category_products":
                bulk_add_category_products(payload)
            elif mode == "order_items":
                bulk_add_order_items(payload)
            else:
                raise ValueError("Недопустимый режим")
            self.redirect("/?ok=" + quote("Массовое добавление выполнено"))
        except Exception as e:
            logger.exception("Ошибка массового добавления: mode=%s error=%s", mode, e)
            self.redirect("/bulk-add?err=" + quote("Ошибка массового добавления"))

    @require_auth
    def handle_update_get(self):
        body = f"""
        <div class='actions nav'><a href='/'>Назад</a></div>
        {self.get_message()}
        <div class='section'>
          <h2>Обновление одной записи</h2>
          <form method='post' action='/update'>
            <label>Таблица</label>
            <select name='table'>
              <option value='categories'>Категории</option>
              <option value='products'>Товары</option>
              <option value='customers'>Клиенты</option>
            </select>
            <label>Идентификатор записи</label>
            <input name='match_value' required placeholder='Для категории и товара — name, для клиента — email'>
            <p class='small'>Категории ищутся по name, товары по name, клиенты по email.</p>
            <label>Новые значения JSON</label>
            <textarea name='payload' required placeholder='{{&quot;stock&quot;:&quot;15&quot;,&quot;price&quot;:&quot;1200.00&quot;}}'></textarea>
            <div class='actions'><button class='btn' type='submit'>Обновить</button></div>
          </form>
        </div>
        """
        self.send_html(html_page("Обновление", body, self.current_user))

    @require_auth
    def handle_update_post(self):
        form = self.parse_post_data()
        table = form.get("table", "")
        match_value = form.get("match_value", "")
        logger.info("Обновление одной записи: table=%s match_value=%s user=%s ip=%s", table, match_value, self.current_user, self.client_address[0])
        try:
            payload = json.loads(form.get("payload", "{}"))
            update_single(table, match_value, payload)
            self.redirect("/?ok=" + quote("Запись успешно обновлена"))
        except Exception as e:
            logger.exception("Ошибка обновления одной записи: table=%s match_value=%s error=%s", table, match_value, e)
            self.redirect("/update?err=" + quote("Ошибка обновления записи"))

    @require_auth
    def handle_bulk_update_get(self):
        body = f"""
        <div class='actions nav'><a href='/'>Назад</a></div>
        {self.get_message()}
        <div class='section'>
          <h2>Пакетное обновление</h2>
          <form method='post' action='/bulk-update'>
            <label>Таблица</label>
            <select name='table'>
              <option value='products'>Товары</option>
              <option value='orders'>Заказы</option>
              <option value='customers'>Клиенты</option>
            </select>
            <label>Колонка фильтра</label>
            <input name='filter_column' placeholder='id или email' required>
            <label>Список значений фильтра через запятую</label>
            <textarea name='filter_values' required placeholder='1,2,3'></textarea>
            <label>Новые значения JSON</label>
            <textarea name='payload' required placeholder='{{&quot;status&quot;:&quot;shipped&quot;}}'></textarea>
            <div class='actions'><button class='btn' type='submit'>Выполнить пакетное обновление</button></div>
            <p class='small'>Для группы однотипных значений используется безопасное формирование списка плейсхолдеров.</p>
          </form>
        </div>
        """
        self.send_html(html_page("Пакетное обновление", body, self.current_user))

    @require_auth
    def handle_bulk_update_post(self):
        form = self.parse_post_data()
        table = form.get("table", "")
        filter_column = form.get("filter_column", "")
        raw_values = form.get("filter_values", "")
        logger.info("Пакетное обновление: table=%s filter_column=%s user=%s ip=%s", table, filter_column, self.current_user, self.client_address[0])
        try:
            payload = json.loads(form.get("payload", "{}"))
            filter_values = [v.strip() for v in raw_values.split(",") if v.strip()]
            bulk_update(table, filter_column, filter_values, payload)
            self.redirect("/?ok=" + quote("Пакетное обновление выполнено"))
        except Exception as e:
            logger.exception("Ошибка пакетного обновления: table=%s filter_column=%s error=%s", table, filter_column, e)
            self.redirect("/bulk-update?err=" + quote("Ошибка пакетного обновления"))


def run():
    server = HTTPServer((APP_HOST, APP_PORT), AppHandler)
    logger.info("Приложение запущено host=%s port=%s", APP_HOST, APP_PORT)
    logger.info("Подробный лог приложения: %s", LOG_FILE)
    logger.info("Лог аутентификации: %s", AUTH_LOG_FILE)
    server.serve_forever()


if __name__ == "__main__":
    run()
