-- Категории
INSERT INTO categories (name, description) VALUES
('Электроника', 'Электронные устройства и гаджеты'),
('Одежда', 'Одежда и аксессуары'),
('Книги', 'Печатные и электронные книги'),
('Мебель', 'Мебель для дома и офиса');

-- Товары
INSERT INTO products (name, description, category_id, price, stock) VALUES
('Ноутбук Dell XPS 13', 'Мощный ноутбук для работы', 1, 1200.00, 5),
('iPhone 14 Pro', 'Смартфон Apple последнего поколения', 1, 1000.00, 3),
('Футболка Nike', 'Комфортная спортивная футболка', 2, 35.00, 20),
('Джинсы Levi''s', 'Классические синие джинсы', 2, 60.00, 15),
('SQL для начинающих', 'Учебник по основам SQL', 3, 25.00, 10),
('Письменный стол', 'Деревянный стол для работы', 4, 200.00, 8);

-- Клиенты
INSERT INTO customers (first_name, last_name, email, phone, address) VALUES
('Иван', 'Петров', 'ivan.petrov@example.com', '+7-900-123-45-67', 'Москва, ул. Главная, д. 1'),
('Мария', 'Сидорова', 'maria.sidorova@example.com', '+7-900-234-56-78', 'СПб, ул. Невский, д. 50'),
('Александр', 'Иванов', 'alex.ivanov@example.com', '+7-900-345-67-89', 'Казань, ул. Центральная, д. 25'),
('Елена', 'Козлова', 'elena.kozlova@example.com', '+7-900-456-78-90', 'Новосибирск, пр. Красный, д. 10');

-- Заказы и их детали
INSERT INTO orders (customer_id, status) VALUES (1, 'delivered'), (2, 'pending'), (3, 'shipped');

INSERT INTO order_items (order_id, product_id, quantity, price) VALUES
(1, 1, 1, 1200.00),
(1, 3, 2, 35.00),
(2, 2, 1, 1000.00),
(3, 5, 2, 25.00),
(3, 6, 1, 200.00);

-- Обновление суммы заказов
UPDATE orders SET total_amount = (
    SELECT COALESCE(SUM(quantity * price), 0) FROM order_items WHERE order_items.order_id = orders.id
);
