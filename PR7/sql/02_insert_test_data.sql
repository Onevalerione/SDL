INSERT INTO categories (name, description) VALUES
('Электроника', 'Электронные устройства и гаджеты'),
('Одежда', 'Одежда и аксессуары'),
('Книги', 'Печатные и электронные книги');

INSERT INTO products (name, description, category_id, price, stock) VALUES
('Ноутбук Dell XPS 13', 'Мощный ноутбук для работы', 1, 1200.00, 5),
('iPhone 14 Pro', 'Смартфон Apple последнего поколения', 1, 1000.00, 3),
('Футболка Nike', 'Комфортная спортивная футболка', 2, 35.00, 20),
('SQL для начинающих', 'Учебник по основам SQL', 3, 25.00, 10);

INSERT INTO customers (first_name, last_name, email, phone, address) VALUES
('Иван', 'Петров', 'ivan.petrov@example.com', '+7-900-123-45-67', 'Москва, ул. Главная, д. 1'),
('Мария', 'Сидорова', 'maria.sidorova@example.com', '+7-900-234-56-78', 'Санкт-Петербург, Невский пр., д. 50');

INSERT INTO orders (customer_id, status) VALUES (1, 'delivered'), (2, 'pending');
INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (1, 1, 1, 1200.00), (1, 3, 2, 35.00), (2, 2, 1, 1000.00);
UPDATE orders SET total_amount = (SELECT COALESCE(SUM(quantity * price), 0) FROM order_items WHERE order_items.order_id = orders.id);
