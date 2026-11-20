-- Create (or recreate) bench_items with :n rows. Run: psql -v n=100000 -f create_items.sql
-- A row is about 300 bytes as JSON: short text columns, a 100-character note, a small nested object.
DROP TABLE IF EXISTS bench_items;
CREATE TABLE bench_items (
    id         bigint PRIMARY KEY,
    name       text,
    email      text,
    amount     numeric(12,2),
    status     text,
    notes      text,
    payload    jsonb,
    updated_at timestamptz
);
INSERT INTO bench_items
SELECT g,
       'item ' || g,
       'user' || g || '@example.com',
       ((g * 37) % 1000000) / 100.0,
       (ARRAY['new','active','paused','closed'])[1 + g % 4],
       left(repeat(md5(g::text), 4), 100),
       jsonb_build_object('tags', jsonb_build_array('t' || (g % 7), 't' || (g % 11)),
                          'score', g % 100,
                          'geo', jsonb_build_object('lat', 40 + (g % 1000) / 1000.0, 'lon', -3 - (g % 1000) / 1000.0)),
       timestamptz '2026-01-01 00:00:00+00' + (g || ' seconds')::interval
FROM generate_series(1, :n) AS g;
ANALYZE bench_items;
SELECT count(*) AS rows_in_source FROM bench_items;
