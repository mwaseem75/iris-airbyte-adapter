-- Synthetic demo data only.
CREATE TABLE customers (
    id         int PRIMARY KEY,
    name       text,
    email      text,
    balance    numeric(12,2),
    updated_at timestamptz
);

INSERT INTO customers (id, name, email, balance, updated_at) VALUES
    (1, 'Alice Smith',  'alice@example.com', 120.50,  '2026-11-01 10:00:00+00'),
    (2, 'Zoë Müller',   'zoe@example.com',   0.00,    '2026-11-02 09:30:00+01'),
    (3, 'محمد',          'mohammed@example.com', 1999.99, '2026-11-03 12:15:00+03'),
    (4, 'Bob Jones',    NULL,                -45.10,  '2026-11-04 08:00:00+00'),
    (5, 'Chen Wei',     'chen@example.com',  10000000.00, '2026-11-05 23:59:59-05');

CREATE TABLE order_lines (
    order_id int,
    line_no  int,
    product  text,
    qty      int,
    PRIMARY KEY (order_id, line_no)
);

-- order_id 1..3 deliberately reuse customers ids (same key in two streams)
INSERT INTO order_lines (order_id, line_no, product, qty) VALUES
    (1, 1, 'Widget',   3),
    (1, 2, 'Gadget',   1),
    (2, 1, 'Café Crème', 2),
    (3, 1, 'Widget',   10),
    (4, 1, 'Gizmo',    5);

-- Supplier ids 1-3 deliberately collide with customer ids 1-3,
-- so the same key ["1"] exists in two streams and both rows are stored in IRIS (isolation by stream).
CREATE TABLE suppliers (
    id         int PRIMARY KEY,
    name       text,
    updated_at timestamptz
);

INSERT INTO suppliers (id, name, updated_at) VALUES
    (1, 'Acme Supplies',        '2026-10-20 09:00:00+00'),
    (2, 'Müller & Söhne GmbH',  '2026-10-21 10:30:00+01'),
    (3, 'شركة النور',            '2026-10-22 12:00:00+03');
