-- Live change 3: change an order line (order_lines has no cursor column, it is re-read in full every run).
UPDATE order_lines SET qty = qty + 1 WHERE order_id = 1 AND line_no = 1;
SELECT order_id, line_no, product, qty FROM order_lines WHERE order_id = 1 AND line_no = 1;
