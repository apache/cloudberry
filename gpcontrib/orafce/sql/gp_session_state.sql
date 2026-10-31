-- Settings made by plvdate.set_*, plvsubst.setsubst() and
-- oracle.set_nls_sort() must be seen by the segments, which evaluate these
-- functions over table data.
CREATE TABLE gp_state_t (id int, d date, s text) DISTRIBUTED BY (id);
INSERT INTO gp_state_t VALUES (1, '2025-12-31', 'x=$$ y=%s'), (2, '2025-12-30', 'x=$$ y=%s');

SELECT plvdate.set_nonbizday('2025-12-31'::date, false);
SELECT plvdate.isbizday('2025-12-31'::date) AS coordinator;
SELECT id, plvdate.isbizday(d) FROM gp_state_t ORDER BY id;
-- a reader gang
SELECT a.id, plvdate.isbizday(b.d)
  FROM gp_state_t a JOIN gp_state_t b ON a.d = b.d ORDER BY a.id;
-- every segment has the same calendar
SELECT count(DISTINCT current_setting('orafce.plvdate_state')) AS calendars
  FROM gp_dist_random('gp_id');

-- a rolled back change is undone everywhere
BEGIN;
SELECT plvdate.unset_nonbizday('2025-12-31'::date, false);
SELECT id, plvdate.isbizday(d) FROM gp_state_t ORDER BY id;
ROLLBACK;
SELECT id, plvdate.isbizday(d) FROM gp_state_t ORDER BY id;

SELECT plvdate.default_holidays('czech');
SELECT plvdate.isbizday('2025-12-24'::date) AS coordinator;
SELECT id, plvdate.isbizday(d - 7) FROM gp_state_t ORDER BY id;
SELECT plvdate.include_start(false);
SELECT plvdate.including_start();
SELECT DISTINCT current_setting('orafce.plvdate_state') = (SELECT current_setting('orafce.plvdate_state')) AS same
  FROM gp_dist_random('gp_id');

-- the calendar is checked when it is set directly
SET orafce.plvdate_state = 'nonsense';

SELECT plvsubst.setsubst('$$');
SELECT plvsubst.string('x=$$ y=%s', 'A') AS coordinator;
SELECT id, plvsubst.string(s, 'A') FROM gp_state_t ORDER BY id;
SELECT plvsubst.setsubst();
SELECT id, plvsubst.string(s, 'A') FROM gp_state_t ORDER BY id;

SELECT oracle.set_nls_sort('C');
SELECT DISTINCT current_setting('orafce.nls_sort') FROM gp_dist_random('gp_id');
SELECT oracle.set_nls_sort('');

DROP TABLE gp_state_t;
