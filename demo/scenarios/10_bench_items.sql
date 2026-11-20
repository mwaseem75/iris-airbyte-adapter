-- Temporary larger table for testing async reads (50,000 rows). Remove with 11_drop_bench_items.sql.
DROP TABLE IF EXISTS bench_items;
CREATE TABLE bench_items (
    id         int PRIMARY KEY,
    name       text,
    amount     numeric(12,2),
    updated_at timestamptz
);
INSERT INTO bench_items (id, name, amount, updated_at)
SELECT g, 'item ' || g, (g % 1000) + 0.25, '2026-11-01 00:00:00+00'::timestamptz + (g || ' seconds')::interval
FROM generate_series(1, 50000) AS g;
SELECT count(*) AS bench_items FROM bench_items;
