import json
import os
import getpass
import re
import sys
import psycopg2
import threading
import time

def load_config():
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка чтения конфигурационного файла: {e}")
        sys.exit(1)

def validate_username_password(username, password):
    if not re.match(r'^\w+$', username):
        print("Ошибка: логин содержит недопустимые символы (разрешены только буквы, цифры и _ )")
        sys.exit(1)
    if not password:
        print("Ошибка: пароль не может быть пустым")
        sys.exit(1)

def password_input_with_timeout(prompt="Введите пароль: ", timeout=15):
    password = [None]
    def worker():
        password[0] = getpass.getpass(prompt)
    t = threading.Thread(target=worker)
    t.daemon = True
    t.start()
    start = time.time()
    while t.is_alive():
        t.join(0.1)
        if time.time() - start > timeout:
            print("\nТаймаут: не удалось ввести пароль")
            sys.exit(1)
    return password[0] if password[0] is not None else ""

def get_credentials():
    username = input("Введите логин: ").strip()
    password = password_input_with_timeout("Введите пароль: ", timeout=15)
    validate_username_password(username, password)
    return username, password

def connect_and_query():
    config = load_config()
    username, password = get_credentials()
    params = {
        'host': config.get('host', 'localhost'),
        'port': config.get('port', 5432),
        'database': config.get('database', ''),
        'user': username,
        'password': password
    }
    try:
        with psycopg2.connect(**params) as conn:
            print("Подключение к базе данных прошло успешно!")
            with conn.cursor() as cursor:
                cursor.execute("SELECT VERSION();")
                version = cursor.fetchone()
                print("Версия PostgreSQL:", version[0])
    except Exception as e:
        print(f"Ошибка при подключении или выполнении запроса: {e}")

if __name__ == "__main__":
    connect_and_query()
