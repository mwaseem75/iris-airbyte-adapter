-- Live change 2: add a customer.
INSERT INTO customers (id, name, email, balance, updated_at)
VALUES (6, 'Dana Lee', 'dana@example.com', 5.00, now());
SELECT id, name, balance, updated_at FROM customers WHERE id = 6;
