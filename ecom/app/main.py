#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import psycopg2
from psycopg2 import sql, Error
from dotenv import load_dotenv
import logging
from datetime import datetime
from getpass import getpass  

load_dotenv()

LOG_FILE = os.getenv('LOG_FILE')
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

if LOG_FILE:
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )
else:
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

logger = logging.getLogger(__name__)

def get_db_connection():
    """Подключение к БД: приоритет переменные окружения → интерактивный ввод"""
    try:
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        database = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        
        print("\n" + "="*50)
        print("ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ")
        print("="*50)
        
        if all([host, port, database, user]):
            print(f"✓ Найдены переменные окружения:")
            print(f"  Хост: {host}")
            print(f"  Порт: {port}")
            print(f"  База: {database}")
            print(f"  Пользователь: {user}")
            
            use_env = input("Использовать эти настройки? (y/n): ").strip().lower()
            if use_env != 'n':
                print("✓ Используются переменные окружения")
        else:
            print("⚠ Переменные окружения не найдены")
        
        print("\nВведите учётные данные для БД:")
        host = host or input("Хост БД (по умолчанию localhost): ").strip() or 'localhost'
        port = port or input("Порт БД (по умолчанию 5432): ").strip() or '5432'
        database = database or input("Название БД: ").strip()
        user = user or input("Логин БД: ").strip()
        
        password = password or getpass("Пароль БД (не отображается): ").strip()
        
        print(f"\nПодключение к {database}@{host}:{port} как {user}...")
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()[0]
        cursor.close()
        
        logger.info(f"✓ Успешное подключение к {database}@{host}:{port} (пользователь: {user})")
        print(f"✓ Успешное подключение к '{database}' (PostgreSQL: {db_version[:50]}...)")
        print("="*50)
        return conn
        
    except Error as e:
        logger.error(f"✗ Ошибка подключения: {str(e)}")
        print(f"✗ Ошибка подключения к БД:")
        print(f"  • Проверьте, запущен ли PostgreSQL")
        print(f"  • Проверьте учётные данные")
        print(f"  • Проверьте доступность хоста {host}:{port}")
        sys.exit(1)

def execute_select(conn, table_name, filters=None, columns=None):
    """
    Выполнение SELECT запроса с опциональной фильтрацией
    filters: None или список кортежей (column, value)
    columns: None (все колонки) или список названий колонок
    """
    try:
        cursor = conn.cursor()
        
        valid_tables = ['categories', 'products', 'customers', 'orders', 'order_items']
        if table_name not in valid_tables:
            logger.warning(f"Попытка запроса к недопустимой таблице: {table_name}")
            print(f"✗ Таблица '{table_name}' не найдена")
            return None

        # Формирование списка колонок
        if columns:
            cursor.execute(f"""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = %s
            """, (table_name,))
            valid_columns = [row[0] for row in cursor.fetchall()]
            
            selected_cols = []
            for col in columns:
                if col in valid_columns:
                    selected_cols.append(sql.Identifier(col))
                else:
                    logger.warning(f"Недопустимая колонка {col} для таблицы {table_name}")
                    print(f"✗ Колонка '{col}' не найдена в таблице '{table_name}'")
                    return None
            
            column_list = sql.SQL(', ').join(selected_cols)
        else:
            column_list = sql.SQL('*')

        where_clause = sql.SQL('')
        params = []
        
        if filters:
            conditions = []
            for filter_col, filter_val in filters:
                # Проверка названия колонки
                cursor.execute(f"""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = %s AND column_name = %s
                """, (table_name, filter_col))
                
                if not cursor.fetchone():
                    logger.warning(f"Недопустимая колонка для фильтрации: {filter_col}")
                    print(f"✗ Колонка для фильтрации '{filter_col}' не найдена")
                    return None
                
                conditions.append(sql.SQL('{} = %s').format(sql.Identifier(filter_col)))
                params.append(filter_val)
            
            where_clause = sql.SQL(' WHERE ') + sql.SQL(' AND ').join(conditions)

        # Формирование и выполнение запроса
        query = sql.SQL('SELECT {} FROM {}{}').format(
            column_list,
            sql.Identifier(table_name),
            where_clause
        )
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        
        logger.info(f"SELECT запрос к таблице {table_name}: получено {len(rows)} строк")
        return {'columns': col_names, 'rows': rows}
        
    except Error as e:
        logger.error(f"Ошибка при выполнении SELECT: {str(e)}")
        print("✗ Ошибка при выполнении запроса к БД")
        return None
    finally:
        cursor.close()

# Функция для INSERT одной строки
def execute_insert_single(conn, table_name, data):
    """
    Вставка одной строки в таблицу
    data: словарь {column: value}
    """
    try:
        cursor = conn.cursor()
        
        valid_tables = ['categories', 'products', 'customers', 'orders', 'order_items']
        if table_name not in valid_tables:
            print(f"✗ Таблица '{table_name}' не найдена")
            return None

        # Получение валидных колонок
        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = %s AND column_name != 'id'
        """, (table_name,))
        valid_columns = [row[0] for row in cursor.fetchall()]

        # Фильтрация данных (убирание id и невалидных колонок)
        filtered_data = {k: v for k, v in data.items() if k in valid_columns}
        
        if not filtered_data:
            print("✗ Нет валидных данных для вставки")
            return None

        columns = sql.SQL(', ').join(map(sql.Identifier, filtered_data.keys()))
        values = sql.SQL(', ').join(sql.Placeholder() * len(filtered_data))
        
        query = sql.SQL('INSERT INTO {} ({}) VALUES ({}) RETURNING id').format(
            sql.Identifier(table_name),
            columns,
            values
        )
        
        cursor.execute(query, list(filtered_data.values()))
        inserted_id = cursor.fetchone()[0]
        conn.commit()
        
        logger.info(f"INSERT в таблицу {table_name}: добавлена строка с id={inserted_id}")
        print(f"✓ Строка успешно добавлена с id={inserted_id}")
        return inserted_id
        
    except Error as e:
        conn.rollback()
        logger.error(f"Ошибка при INSERT: {str(e)}")
        print("✗ Ошибка при добавлении строки в БД")
        return None
    finally:
        cursor.close()

# Функция для INSERT в несколько связанных таблиц
def execute_insert_related(conn, operations):
    """
    Вставка в несколько связанных таблиц
    operations: список кортежей [(table_name, data), ...]
    """
    try:
        cursor = conn.cursor()
        inserted_ids = {}
        
        for idx, (table_name, data) in enumerate(operations):
            valid_tables = ['categories', 'products', 'customers', 'orders', 'order_items']
            if table_name not in valid_tables:
                print(f"✗ Таблица '{table_name}' не найдена")
                conn.rollback()
                return None

            # Получение валидных колонок
            cursor.execute(f"""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = %s AND column_name != 'id'
            """, (table_name,))
            valid_columns = [row[0] for row in cursor.fetchall()]

            # Замена ссылок на ID из предыдущих операций
            for key in list(data.keys()):
                if isinstance(data[key], str) and data[key].startswith('$'):
                    ref_key = data[key][1:]
                    if ref_key in inserted_ids:
                        data[key] = inserted_ids[ref_key]

            # Фильтрация данных
            filtered_data = {k: v for k, v in data.items() if k in valid_columns}
            
            if not filtered_data:
                print(f"✗ Нет валидных данных для вставки в {table_name}")
                conn.rollback()
                return None

            columns = sql.SQL(', ').join(map(sql.Identifier, filtered_data.keys()))
            values = sql.SQL(', ').join(sql.Placeholder() * len(filtered_data))
            
            query = sql.SQL('INSERT INTO {} ({}) VALUES ({}) RETURNING id').format(
                sql.Identifier(table_name),
                columns,
                values
            )
            
            cursor.execute(query, list(filtered_data.values()))
            inserted_id = cursor.fetchone()[0]
            inserted_ids[f'{table_name}_{idx}'] = inserted_id
            
            logger.info(f"INSERT в таблицу {table_name}: добавлена строка с id={inserted_id}")
        
        conn.commit()
        print(f"✓ Успешно добавлены {len(operations)} строк в связанные таблицы")
        return inserted_ids
        
    except Error as e:
        conn.rollback()
        logger.error(f"Ошибка при INSERT в связанные таблицы: {str(e)}")
        print("✗ Ошибка при добавлении строк. Все изменения отменены.")
        return None
    finally:
        cursor.close()

# Функция для UPDATE одной записи
def execute_update_single(conn, table_name, record_id, updates):
    """
    Обновление одной записи
    updates: словарь {column: new_value}
    """
    try:
        cursor = conn.cursor()
        
        valid_tables = ['categories', 'products', 'customers', 'orders', 'order_items']
        if table_name not in valid_tables:
            print(f"✗ Таблица '{table_name}' не найдена")
            return False

        # Получение валидных колонок (кроме id)
        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = %s AND column_name != 'id'
        """, (table_name,))
        valid_columns = [row[0] for row in cursor.fetchall()]

        # Фильтрация обновлений
        filtered_updates = {k: v for k, v in updates.items() if k in valid_columns}
        
        if not filtered_updates:
            print("✗ Нет валидных данных для обновления")
            return False

        set_clause = sql.SQL(', ').join(
            sql.SQL('{} = %s').format(sql.Identifier(k)) for k in filtered_updates.keys()
        )
        
        query = sql.SQL('UPDATE {} SET {} WHERE id = %s').format(
            sql.Identifier(table_name),
            set_clause
        )
        
        values = list(filtered_updates.values()) + [record_id]
        cursor.execute(query, values)
        conn.commit()
        
        if cursor.rowcount > 0:
            logger.info(f"UPDATE таблицы {table_name}: обновлена запись id={record_id}")
            print(f"✓ Запись id={record_id} успешно обновлена")
            return True
        else:
            print(f"✗ Запись с id={record_id} не найдена")
            return False
        
    except Error as e:
        conn.rollback()
        logger.error(f"Ошибка при UPDATE: {str(e)}")
        print("✗ Ошибка при обновлении записи в БД")
        return False
    finally:
        cursor.close()

# Функция для UPDATE нескольких записей
def execute_update_multiple(conn, table_name, filter_col, filter_values, updates):
    """
    Обновление нескольких записей по условию
    filter_values: список значений для фильтрации
    """
    try:
        cursor = conn.cursor()
        
        valid_tables = ['categories', 'products', 'customers', 'orders', 'order_items']
        if table_name not in valid_tables:
            print(f"✗ Таблица '{table_name}' не найдена")
            return False

        # Валидация колонки фильтрации
        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = %s AND column_name = %s
        """, (table_name, filter_col))
        
        if not cursor.fetchone():
            print(f"✗ Колонка '{filter_col}' не найдена")
            return False

        # Получение валидных колонок для обновления
        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = %s AND column_name != 'id'
        """, (table_name,))
        valid_columns = [row[0] for row in cursor.fetchall()]

        filtered_updates = {k: v for k, v in updates.items() if k in valid_columns}
        
        if not filtered_updates:
            print("✗ Нет валидных данных для обновления")
            return False

        set_clause = sql.SQL(', ').join(
            sql.SQL('{} = %s').format(sql.Identifier(k)) for k in filtered_updates.keys()
        )
        
        placeholders = sql.SQL(', ').join(sql.Placeholder() * len(filter_values))
        
        query = sql.SQL('UPDATE {} SET {} WHERE {} IN ({})').format(
            sql.Identifier(table_name),
            set_clause,
            sql.Identifier(filter_col),
            placeholders
        )
        
        values = list(filtered_updates.values()) + filter_values
        cursor.execute(query, values)
        conn.commit()
        
        logger.info(f"UPDATE таблицы {table_name}: обновлено {cursor.rowcount} записей")
        print(f"✓ Обновлено {cursor.rowcount} записей")
        return True
        
    except Error as e:
        conn.rollback()
        logger.error(f"Ошибка при UPDATE нескольких записей: {str(e)}")
        print("✗ Ошибка при обновлении записей в БД")
        return False
    finally:
        cursor.close()

# Функция для вывода результатов
def print_results(result):
    """Красивый вывод результатов SELECT запроса"""
    if result is None:
        return
    
    columns = result['columns']
    rows = result['rows']
    
    if not rows:
        print("Нет данных для отображения")
        return
    
    # Вычисление ширины колонок
    col_widths = [len(str(col)) for col in columns]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))
    
    # Вывод заголовка
    header = ' | '.join(str(col).ljust(col_widths[i]) for i, col in enumerate(columns))
    print('\n' + header)
    print('-' * len(header))
    
    # Вывод строк
    for row in rows:
        print(' | '.join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)))
    print()

# Интерактивное меню
def interactive_menu(conn):
    """Главное интерактивное меню приложения"""
    while True:
        print("\n" + "="*60)
        print("E-COMMERCE ПРИЛОЖЕНИЕ УПРАВЛЕНИЯ БД")
        print("="*60)
        print("1. Просмотр данных (SELECT)")
        print("2. Добавление данных (INSERT)")
        print("3. Обновление данных (UPDATE)")
        print("4. Выход")
        print("-"*60)
        
        choice = input("Выберите операцию (1-4): ").strip()
        
        if choice == '1':
            select_menu(conn)
        elif choice == '2':
            insert_menu(conn)
        elif choice == '3':
            update_menu(conn)
        elif choice == '4':
            print("Выход из приложения...")
            logger.info("Приложение завершено пользователем")
            break
        else:
            print("✗ Некорректный выбор. Попробуйте снова.")

# Меню SELECT операций
def select_menu(conn):
    """Меню для операций SELECT"""
    print("\n--- ПРОСМОТР ДАННЫХ (SELECT) ---")
    
    tables = ['categories', 'products', 'customers', 'orders', 'order_items']
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    print("0. Вернуться в главное меню")
    
    choice = input("Выберите таблицу: ").strip()
    
    if choice == '0':
        return
    
    try:
        table_idx = int(choice) - 1
        if 0 <= table_idx < len(tables):
            table_name = tables[table_idx]
        else:
            print("✗ Некорректный выбор")
            return
    except ValueError:
        print("✗ Введите число")
        return
    
    print("\n1. Все данные")
    print("2. Фильтрация по одному полю")
    print("3. Фильтрация по нескольким полям")
    print("0. Вернуться")
    
    filter_choice = input("Выберите тип запроса: ").strip()
    
    if filter_choice == '0':
        return
    elif filter_choice == '1':
        result = execute_select(conn, table_name)
        print_results(result)
    elif filter_choice == '2':
        col = input("Введите название колонки: ").strip()
        val = input("Введите значение для фильтрации: ").strip()
        result = execute_select(conn, table_name, filters=[(col, val)])
        print_results(result)
    elif filter_choice == '3':
        filters = []
        num_filters = int(input("Сколько условий фильтрации? "))
        for i in range(num_filters):
            col = input(f"Условие {i+1} - колонка: ").strip()
            val = input(f"Условие {i+1} - значение: ").strip()
            filters.append((col, val))
        result = execute_select(conn, table_name, filters=filters)
        print_results(result)
    else:
        print("✗ Некорректный выбор")

# Меню INSERT операций
def insert_menu(conn):
    """Меню для операций INSERT"""
    print("\n--- ДОБАВЛЕНИЕ ДАННЫХ (INSERT) ---")
    
    print("1. Добавить одну строку в таблицу")
    print("2. Добавить строки в несколько связанных таблиц")
    print("0. Вернуться")
    
    choice = input("Выберите опцию: ").strip()
    
    if choice == '0':
        return
    elif choice == '1':
        insert_single(conn)
    elif choice == '2':
        insert_related(conn)
    else:
        print("✗ Некорректный выбор")

def insert_single(conn):
    """Добавление одной строки"""
    tables = ['categories', 'products', 'customers', 'orders', 'order_items']
    print("\nДоступные таблицы:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    choice = input("Выберите таблицу: ").strip()
    
    try:
        table_idx = int(choice) - 1
        if 0 <= table_idx < len(tables):
            table_name = tables[table_idx]
        else:
            print("✗ Некорректный выбор")
            return
    except ValueError:
        print("✗ Введите число")
        return
    
    data = {}
    print(f"\nВведите данные для таблицы '{table_name}' (или оставьте пусто для пропуска):")
    
    # Примеры полей для каждой таблицы
    if table_name == 'categories':
        data['name'] = input("name: ").strip()
        data['description'] = input("description: ").strip()
    elif table_name == 'products':
        data['name'] = input("name: ").strip()
        data['description'] = input("description: ").strip()
        data['category_id'] = input("category_id: ").strip()
        data['price'] = input("price: ").strip()
        data['stock'] = input("stock: ").strip()
    elif table_name == 'customers':
        data['first_name'] = input("first_name: ").strip()
        data['last_name'] = input("last_name: ").strip()
        data['email'] = input("email: ").strip()
        data['phone'] = input("phone: ").strip()
        data['address'] = input("address: ").strip()
    elif table_name == 'orders':
        data['customer_id'] = input("customer_id: ").strip()
        data['status'] = input("status (pending/shipped/delivered/cancelled): ").strip()
    elif table_name == 'order_items':
        data['order_id'] = input("order_id: ").strip()
        data['product_id'] = input("product_id: ").strip()
        data['quantity'] = input("quantity: ").strip()
        data['price'] = input("price: ").strip()
    
    # Удаление пустых значений
    data = {k: v for k, v in data.items() if v}
    
    execute_insert_single(conn, table_name, data)

def insert_related(conn):
    """Добавление в связанные таблицы"""
    print("\nПримеры связанных таблиц:")
    print("1. Order → Order Items (заказ и его позиции)")
    print("2. Category → Products (категория и товары)")
    print("0. Вернуться")
    
    choice = input("Выберите сценарий: ").strip()
    
    if choice == '0':
        return
    elif choice == '1':
        # Order → Order Items
        operations = []
        
        print("\n--- Шаг 1: Добавить заказ ---")
        customer_id = input("customer_id: ").strip()
        status = input("status (по умолчанию 'pending'): ").strip() or 'pending'
        
        operations.append(('orders', {
            'customer_id': customer_id,
            'status': status
        }))
        
        print("\n--- Шаг 2: Добавить позицию заказа ---")
        product_id = input("product_id: ").strip()
        quantity = input("quantity: ").strip()
        price = input("price: ").strip()
        
        operations.append(('order_items', {
            'order_id': '$orders_0',  # Ссылка на ID из первой операции
            'product_id': product_id,
            'quantity': quantity,
            'price': price
        }))
        
        execute_insert_related(conn, operations)
    else:
        print("✗ Некорректный выбор")

# Меню UPDATE операций
def update_menu(conn):
    """Меню для операций UPDATE"""
    print("\n--- ОБНОВЛЕНИЕ ДАННЫХ (UPDATE) ---")
    
    print("1. Обновить одну запись")
    print("2. Обновить несколько записей")
    print("0. Вернуться")
    
    choice = input("Выберите опцию: ").strip()
    
    if choice == '0':
        return
    elif choice == '1':
        update_single(conn)
    elif choice == '2':
        update_multiple(conn)
    else:
        print("✗ Некорректный выбор")

def update_single(conn):
    """Обновление одной записи"""
    tables = ['categories', 'products', 'customers', 'orders', 'order_items']
    print("\nДоступные таблицы:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    choice = input("Выберите таблицу: ").strip()
    
    try:
        table_idx = int(choice) - 1
        if 0 <= table_idx < len(tables):
            table_name = tables[table_idx]
        else:
            print("✗ Некорректный выбор")
            return
    except ValueError:
        print("✗ Введите число")
        return
    
    record_id = input("Введите ID записи для обновления: ").strip()
    
    updates = {}
    print(f"\nВведите новые значения для таблицы '{table_name}' (или оставьте пусто для пропуска):")
    
    if table_name == 'categories':
        name = input("name: ").strip()
        desc = input("description: ").strip()
        if name: updates['name'] = name
        if desc: updates['description'] = desc
    elif table_name == 'products':
        name = input("name: ").strip()
        desc = input("description: ").strip()
        category = input("category_id: ").strip()
        price = input("price: ").strip()
        stock = input("stock: ").strip()
        if name: updates['name'] = name
        if desc: updates['description'] = desc
        if category: updates['category_id'] = category
        if price: updates['price'] = price
        if stock: updates['stock'] = stock
    elif table_name == 'customers':
        fname = input("first_name: ").strip()
        lname = input("last_name: ").strip()
        email = input("email: ").strip()
        phone = input("phone: ").strip()
        address = input("address: ").strip()
        if fname: updates['first_name'] = fname
        if lname: updates['last_name'] = lname
        if email: updates['email'] = email
        if phone: updates['phone'] = phone
        if address: updates['address'] = address
    elif table_name == 'orders':
        status = input("status: ").strip()
        if status: updates['status'] = status
    elif table_name == 'order_items':
        qty = input("quantity: ").strip()
        price = input("price: ").strip()
        if qty: updates['quantity'] = qty
        if price: updates['price'] = price
    
    execute_update_single(conn, table_name, record_id, updates)

def update_multiple(conn):
    """Обновление нескольких записей"""
    tables = ['categories', 'products', 'customers', 'orders', 'order_items']
    print("\nДоступные таблицы:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    choice = input("Выберите таблицу: ").strip()
    
    try:
        table_idx = int(choice) - 1
        if 0 <= table_idx < len(tables):
            table_name = tables[table_idx]
        else:
            print("✗ Некорректный выбор")
            return
    except ValueError:
        print("✗ Введите число")
        return
    
    filter_col = input("Введите колонку для фильтрации: ").strip()
    values_str = input("Введите значения через запятую: ").strip()
    filter_values = [v.strip() for v in values_str.split(',')]
    
    updates = {}
    print(f"\nВведите новые значения для обновления:")
    
    if table_name == 'products':
        price = input("price: ").strip()
        stock = input("stock: ").strip()
        if price: updates['price'] = price
        if stock: updates['stock'] = stock
    elif table_name == 'orders':
        status = input("status: ").strip()
        if status: updates['status'] = status
    else:
        col = input("Колонка для обновления: ").strip()
        val = input("Новое значение: ").strip()
        if col and val: updates[col] = val
    
    if updates:
        execute_update_multiple(conn, table_name, filter_col, filter_values, updates)
    else:
        print("✗ Нет данных для обновления")

# Главная функция
def main():
    """Точка входа приложения"""
    logger.info("Запуск приложения E-commerce")
    print("\n" + "="*60)
    print("ПРИЛОЖЕНИЕ УПРАВЛЕНИЯ E-COMMERCE БД")
    print("="*60 + "\n")
    
    conn = get_db_connection()
    
    try:
        interactive_menu(conn)
    finally:
        if conn:
            conn.close()
            logger.info("Подключение к БД закрыто")
            print("\n✓ Подключение закрыто")

if __name__ == '__main__':
    main()
