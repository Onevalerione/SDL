-- Создание пользователя приложения (не администратора)
CREATE USER app_user WITH PASSWORD 'secure_password_123';

-- Предоставление прав на подключение к БД
GRANT CONNECT ON DATABASE ecommerce TO app_user;

-- Предоставление прав на использование схемы public
GRANT USAGE ON SCHEMA public TO app_user;

-- Предоставление прав на чтение всех таблиц
GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_user;

-- Предоставление прав на изменение данных
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user;

-- Предоставление прав на использование последовательностей (для SERIAL)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_user;

-- Установка прав по умолчанию для будущих таблиц
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT, UPDATE, DELETE ON TABLES TO app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO app_user;
