-- Live change 1: change a customer. updated_at MUST move forward or the incremental read will not see it.
UPDATE customers SET balance = balance + 10.00, updated_at = now() WHERE id = 1;
SELECT id, name, balance, updated_at FROM customers WHERE id = 1;
