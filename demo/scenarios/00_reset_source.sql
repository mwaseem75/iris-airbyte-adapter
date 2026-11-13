-- Reset the source to the seed data. Also run ##class(Airbyte.Demo.Util).Reset() in IRIS.
-- A full reset is: docker compose down -v ; docker compose up -d
DELETE FROM customers WHERE id > 5;
UPDATE customers SET balance = 120.50, updated_at = '2026-11-01 10:00:00+00' WHERE id = 1;
UPDATE order_lines SET qty = 3 WHERE order_id = 1 AND line_no = 1;
SELECT count(*) AS customers FROM customers;
