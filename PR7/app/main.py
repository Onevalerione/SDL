import html
import logging
import os
import secrets
from http import cookies
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

HOST = "0.0.0.0"
PORT = 5000
LOG_DIR = "/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "auth.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

SESSIONS = {}

ALLOWED_TABLES = {
    "categories": ["name", "description"],
    "products": ["name", "description", "category_id", "price", "stock"],
    "customers": ["first_name", "last_name", "email", "phone", "address"],
    "orders": ["customer_id", "status"],
    "order_items": ["order_id", "product_id", "quantity", "price"],
}

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "ecommerce")


def html_page(title, body, user=None):
    nav = ""
    if user:
        nav = f'''
        <nav>
            <a href="/home">Главная</a>
            <a href="/tables">Просмотр таблиц</a>
            <a href="/add">Добавление</a>
            <a href="/update">Обновление</a>
            <a href="/logout">Выход</a>
        </nav>
        '''
    return f'''<!doctype html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 0; background: #f4f6f9; color: #222; }}
header {{ background: #1f2937; color: white; padding: 16px 24px; }}
nav a {{ color: white; margin-right: 16px; text-decoration: none; }}
main {{ max-width: 1100px; margin: 24px auto; background: white; padding: 24px; border-radius: 12px; box-shadow: 0 2px 10px rgba(0,0,0,.08); }}
input, select, textarea {{ width: 100%; padding: 10px; margin-top: 6px; margin-bottom: 14px; border: 1px solid #ccc; border-radius: 8px; box-sizing: border-box; }}
button, .btn {{ display: inline-block; background: #2563eb; color: white; padding: 10px 16px; border: 0; border-radius: 8px; text-decoration: none; cursor: pointer; }}
table {{ width: 100%; border-collapse: collapse; margin-top: 16px; }}
th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; vertical-align: top; }}
th {{ background: #f3f4f6; }}
.grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; }}
.card {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 16px; background: #fafafa; }}
</style>
</head>
<body>
<header>{nav}</header>
<main>{body}</main>
</body>
</html>'''


def get_conn(username, password):
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=username,
        password=password,
        cursor_factory=RealDictCursor,
    )


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


class AppHandler(BaseHTTPRequestHandler):
    def parse_cookies(self):
        jar = cookies.SimpleCookie()
        if "Cookie" in self.headers:
            jar.load(self.headers["Cookie"])
        return jar

    def get_session(self):
        jar = self.parse_cookies()
        sid = jar.get("sid")
        if sid:
            return sid.value, SESSIONS.get(sid.value)
        return None, None

    def send_html(self, content, status=200, set_cookie=None, location=None):
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        if set_cookie:
            self.send_header("Set-Cookie", set_cookie)
        if location:
            self.send_header("Location", location)
        self.end_headers()
        if content:
            self.wfile.write(content.encode("utf-8"))

    def redirect(self, location, set_cookie=None):
        self.send_html("", status=302, set_cookie=set_cookie, location=location)

    def read_post_data(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode("utf-8")
        return {k: v[0] for k, v in parse_qs(raw).items()}

    def require_auth(self):
        sid, session = self.get_session()
        if not session:
            self.redirect("/login")
            return None
        return sid, session

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/":
            _, session = self.get_session()
            self.redirect("/home" if session else "/login")
            return

        if path == "/login":
            body = '''
            <h1>Вход в приложение</h1>
            <p>Введите логин и пароль пользователя PostgreSQL.</p>
            <form method="post" action="/login">
                <label>Логин</label>
                <input name="username" required>
                <label>Пароль</label>
                <input name="password" type="password" required>
                <button type="submit">Войти</button>
            </form>
            '''
            self.send_html(html_page("Вход", body))
            return

        if path == "/logout":
            sid, _ = self.get_session()
            if sid and sid in SESSIONS:
                del SESSIONS[sid]
            self.redirect("/login", set_cookie="sid=; Path=/; Max-Age=0")
            return

        auth = self.require_auth()
        if not auth:
            return
        sid, session = auth
        username = session["username"]
        password = session["password"]

        if path == "/home":
            body = '''
            <h1>Главная страница</h1>
            <p>Выберите нужное действие.</p>
            <div class="grid">
                <div class="card"><h3>Просмотр таблиц</h3><p>Просмотр содержимого таблиц БД.</p><a class="btn" href="/tables">Открыть</a></div>
                <div class="card"><h3>Добавление записей</h3><p>Добавление новых строк в таблицы.</p><a class="btn" href="/add">Открыть</a></div>
                <div class="card"><h3>Обновление записей</h3><p>Обновление существующих данных.</p><a class="btn" href="/update">Открыть</a></div>
            </div>
            '''
            self.send_html(html_page("Главная", body, user=username))
            return

        if path == "/tables":
            table = query.get("table", ["categories"])[0]
            if table not in ALLOWED_TABLES:
                self.send_html(html_page("Ошибка", "<h1>Недопустимое имя таблицы</h1>", user=username), status=400)
                return
            with get_conn(username, password) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("SELECT * FROM {} ORDER BY id").format(sql.Identifier(table)))
                    rows = cur.fetchall()
            options = "".join([f'<option value="{t}" {"selected" if t == table else ""}>{t}</option>' for t in ALLOWED_TABLES])
            table_html = "<p>Нет данных.</p>"
            if rows:
                headers = "".join([f"<th>{html.escape(k)}</th>" for k in rows[0].keys()]) + "<th>Действие</th>"
                body_rows = []
                for row in rows:
                    cols = "".join([f"<td>{html.escape(str(v))}</td>" for v in row.values()])
                    cols += f'<td><a class="btn" href="/update?table={table}&id={row["id"]}">Изменить</a></td>'
                    body_rows.append(f"<tr>{cols}</tr>")
                table_html = f"<table><thead><tr>{headers}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"
            body = f'''
            <h1>Просмотр таблиц</h1>
            <form method="get" action="/tables">
                <label>Таблица</label>
                <select name="table">{options}</select>
                <button type="submit">Показать</button>
            </form>
            {table_html}
            '''
            self.send_html(html_page("Таблицы", body, user=username))
            return

        if path in ["/add", "/update"]:
            table = query.get("table", ["categories"])[0]
            if table not in ALLOWED_TABLES:
                self.send_html(html_page("Ошибка", "<h1>Недопустимое имя таблицы</h1>", user=username), status=400)
                return
            fields = ALLOWED_TABLES[table]
            options = "".join([f'<option value="{t}" {"selected" if t == table else ""}>{t}</option>' for t in ALLOWED_TABLES])
            form_fields = ""
            record_id = query.get("id", [""])[0]
            record = {}
            if path == "/update" and record_id:
                with get_conn(username, password) as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql.SQL("SELECT * FROM {} WHERE id = %s").format(sql.Identifier(table)), (record_id,))
                        record = cur.fetchone() or {}
            if path == "/update" and not record_id:
                body = f'''
                <h1>Обновление записи</h1>
                <form method="get" action="/update">
                    <label>Таблица</label>
                    <select name="table">{options}</select>
                    <label>ID записи</label>
                    <input type="number" name="id" required>
                    <button type="submit">Загрузить запись</button>
                </form>
                '''
                self.send_html(html_page("Обновление", body, user=username))
                return
            for field in fields:
                value = html.escape(str(record.get(field, "")))
                form_fields += f'<label>{field}</label><input name="{field}" value="{value}" required>'
            hidden_id = f'<input type="hidden" name="id" value="{html.escape(record_id)}">' if record_id else ''
            title = "Добавление записи" if path == "/add" else "Обновление записи"
            body = f'''
            <h1>{title}</h1>
            <form method="get" action="{path}">
                <label>Таблица</label>
                <select name="table" onchange="this.form.submit()">{options}</select>
                {hidden_id}
            </form>
            <form method="post" action="{path}">
                <input type="hidden" name="table" value="{html.escape(table)}">
                {hidden_id}
                {form_fields}
                <button type="submit">Сохранить</button>
            </form>
            '''
            self.send_html(html_page(title, body, user=username))
            return

        self.send_html(html_page("404", "<h1>Страница не найдена</h1>", user=username), status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/login":
            form = self.read_post_data()
            username = form.get("username", "").strip()
            password = form.get("password", "")
            try:
                conn = get_conn(username, password)
                conn.close()
                sid = secrets.token_hex(16)
                SESSIONS[sid] = {"username": username, "password": password}
                self.redirect("/home", set_cookie=f"sid={sid}; Path=/; HttpOnly")
            except Exception as e:
                logging.warning("Неудачная попытка входа user=%s error=%s", username, str(e))
                body = '<h1>Вход в приложение</h1><p>Неверные данные для подключения к БД.</p><a class="btn" href="/login">Назад</a>'
                self.send_html(html_page("Ошибка входа", body), status=401)
            return

        auth = self.require_auth()
        if not auth:
            return
        _, session = auth
        username = session["username"]
        password = session["password"]
        form = self.read_post_data()
        table = form.get("table", "categories")

        if table not in ALLOWED_TABLES:
            self.send_html(html_page("Ошибка", "<h1>Недопустимое имя таблицы</h1>", user=username), status=400)
            return

        fields = ALLOWED_TABLES[table]
        values = [form.get(field) or None for field in fields]

        with get_conn(username, password) as conn:
            with conn.cursor() as cur:
                if path == "/add":
                    q = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
                        sql.Identifier(table),
                        sql.SQL(", ").join(map(sql.Identifier, fields)),
                        sql.SQL(", ").join(sql.Placeholder() * len(fields)),
                    )
                    cur.execute(q, values)
                    new_id = cur.fetchone()["id"]
                    if table == "orders":
                        recalc_order_total(conn, new_id)
                    elif table == "order_items":
                        recalc_order_total(conn, form.get("order_id"))
                elif path == "/update":
                    record_id = form.get("id")
                    set_clause = sql.SQL(", ").join(
                        sql.SQL("{} = %s").format(sql.Identifier(col)) for col in fields
                    )
                    q = sql.SQL("UPDATE {} SET {} WHERE id = %s").format(
                        sql.Identifier(table), set_clause
                    )
                    cur.execute(q, values + [record_id])
                    if table == "products":
                        cur.execute("UPDATE products SET updated_at = CURRENT_TIMESTAMP WHERE id = %s", (record_id,))
                    elif table == "orders":
                        recalc_order_total(conn, record_id)
                    elif table == "order_items":
                        cur.execute("SELECT order_id FROM order_items WHERE id = %s", (record_id,))
                        row = cur.fetchone()
                        if row:
                            recalc_order_total(conn, row["order_id"])
            conn.commit()

        self.redirect(f"/tables?table={table}")


if __name__ == "__main__":
    server = HTTPServer((HOST, PORT), AppHandler)
    print(f"Server started on http://{HOST}:{PORT}")
    server.serve_forever()
