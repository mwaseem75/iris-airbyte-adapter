-- Change the source like a real system would: update :upd_pct % of the rows (new updated_at) and insert
-- :ins_pct % new rows. :seed picks which rows are updated, so repeated calls touch different rows.
-- Run: psql -v upd_pct=10 -v ins_pct=5 -v seed=0 -f mixed_change.sql
WITH total AS (SELECT count(*) AS n, max(id) AS top FROM bench_items)
UPDATE bench_items b
   SET updated_at = now(), amount = b.amount + 1, status = 'changed'
  FROM total
 WHERE (b.id + :seed) % 100 < :upd_pct;
WITH total AS (SELECT count(*) AS n, max(id) AS top FROM bench_items)
INSERT INTO bench_items
SELECT total.top + g,
       'item ' || (total.top + g),
       'user' || (total.top + g) || '@example.com',
       (((total.top + g) * 37) % 1000000) / 100.0,
       'new',
       left(repeat(md5((total.top + g)::text), 4), 100),
       jsonb_build_object('tags', jsonb_build_array('t1'), 'score', g % 100,
                          'geo', jsonb_build_object('lat', 41.0, 'lon', -4.0)),
       now()
FROM total, generate_series(1, (total.n * :ins_pct / 100)::int) AS g;
SELECT count(*) AS rows_in_source FROM bench_items;
