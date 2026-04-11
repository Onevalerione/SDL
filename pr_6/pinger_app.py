#!/usr/bin/env python3
import os
import sys
import time
import psycopg2
import psycopg2.extras
from contextlib import closing

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def getenv_str(name: str, required: bool = False) -> str:
    val = os.getenv(name)
    if required and (val is None or val == ""):
        raise RuntimeError(f"Переменная окружения {name} не задана")
    return val

def getenv_float(name: str, required: bool = False) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        if required:
            raise RuntimeError(f"Переменная окружения {name} не задана")
        return 0
    try:
        return float(raw)
    except ValueError:
        raise RuntimeError(f"Переменная окружения {name} имеет нечисловое значение: {raw!r}")

def open_logfile():
    path = getenv_str("LOG_FILE_PATH")
    if not path:
        return None
    try:
        return open(path, "a", buffering=1, encoding="utf-8")
    except Exception as e:
        eprint(f"[pinger] Не удалось открыть лог-файл {path}: {e}")
        return None

def write_both(fh, msg: str, is_err: bool = False):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {msg}"
    if is_err:
        eprint(line)
    else:
        print(line)
    if fh:
        try:
            fh.write(line + "\n")
        except Exception:
            pass

def build_conn_params():
    params = {
        "host": getenv_str("DB_HOST", required=True),
        "port": int(getenv_str("DB_PORT", required=True)),
        "dbname": getenv_str("DB_NAME", required=True),
        "user": getenv_str("DB_USER", required=True),
        "password": getenv_str("DB_PASSWORD", required=True),
        "connect_timeout": int(getenv_str("DB_CONNECT_TIMEOUT") or 5),
        "options": f"-c statement_timeout={int(getenv_str('DB_STATEMENT_TIMEOUT_MS') or 4000)}",
    }
    sslmode = os.getenv("DB_SSLMODE")
    if sslmode:
        params["sslmode"] = sslmode
    return params

def poll_once(params: dict, logfile):
    try:
        with closing(psycopg2.connect(**params)) as conn:
            with closing(conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)) as cur:
                cur.execute("SELECT version();")
                row = cur.fetchone()
                version = row.get("version") if isinstance(row, dict) else (row[0] if row else None)
                if not version or not isinstance(version, str):
                    write_both(logfile, "[pinger] Нетипичный ответ на SELECT version(): " + repr(row))
                else:
                    if "PostgreSQL" not in version:
                        write_both(logfile, "[pinger] Нетипичный ответ version(): " + version)
                    else:
                        write_both(logfile, "[pinger] Успешное подключение, version(): " + version)
                return True
    except psycopg2.OperationalError as e:
        write_both(logfile, f"[pinger] Ошибка подключения/сети: {e}", is_err=True)
        return False
    except psycopg2.ProgrammingError as e:
        write_both(logfile, f"[pinger] SQL-ошибка: {e}", is_err=True)
        return False
    except Exception as e:
        write_both(logfile, f"[pinger] Неожиданная ошибка: {e}", is_err=True)
        return False

def main():
    logfile = open_logfile()

    try:
        interval_min = getenv_float("PING_INTERVAL_MINUTES", required=True)
    except Exception as e:
        eprint(f"[pinger] Ошибка: {e}")
        sys.exit(1)

    conn_params = build_conn_params()
    interval_sec = max(1.0, interval_min * 60)  # Мин интервал 1 секунда для теста

    write_both(
        logfile,
        f"[pinger] Старт. host={conn_params.get('host')} port={conn_params.get('port')} db={conn_params.get('dbname')} interval={int(interval_sec)}s"
    )

    while True:
        poll_once(conn_params, logfile)
        time.sleep(interval_sec)

if __name__ == "__main__":
    main()
