-- Verify that a distributed snapshot cannot retain a gxid after the QE has
-- committed it.  Session 1 is opened before the fault is installed because
-- the fault is scoped to the isolation2test database.
-- The control connection uses postgres, so install the debug extension there
-- as well.  --load-extension only installs it in isolation2test.
-1U:@db_name postgres: CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

CREATE TABLE issue1465_snapshot (k int, v int) DISTRIBUTED REPLICATED;
INSERT INTO issue1465_snapshot VALUES (1, 400);

1: SELECT 1;
1: BEGIN;
1: UPDATE issue1465_snapshot SET v = 500 WHERE k = 1;
-1U:@db_name postgres: SELECT gp_inject_fault('before_xact_end_procarray', 'suspend', '', 'isolation2test', '', 1, 1, 0, 1);
1&: COMMIT;
-1U:@db_name postgres: SELECT gp_wait_until_triggered_fault('before_xact_end_procarray', 1, 1);

2: BEGIN ISOLATION LEVEL REPEATABLE READ;
2&: SELECT k, v FROM issue1465_snapshot ORDER BY k;

-1U:@db_name postgres: SELECT gp_inject_fault('before_xact_end_procarray', 'resume', 1);
1<:
2<:
-1U:@db_name postgres: SELECT gp_inject_fault('all', 'reset', 1);

-- The same snapshot must retain the committed version after the writer exits.
2: SELECT k, v FROM issue1465_snapshot ORDER BY k;
2: ROLLBACK;

DROP TABLE issue1465_snapshot;

-- The one-phase path also notifies a QE before QD transaction cleanup.
CREATE TABLE issue1465_onephase (k int, v int) DISTRIBUTED BY (k);
INSERT INTO issue1465_onephase VALUES (1, 400);

1: BEGIN;
1: UPDATE issue1465_onephase SET v = 500 WHERE k = 1;
-1U:@db_name postgres: SELECT gp_inject_fault('before_xact_end_procarray', 'suspend', '', 'isolation2test', '', 1, 1, 0, 1);
1&: COMMIT;
-1U:@db_name postgres: SELECT gp_wait_until_triggered_fault('before_xact_end_procarray', 1, 1);

2: BEGIN ISOLATION LEVEL REPEATABLE READ;
2&: SELECT k, v FROM issue1465_onephase WHERE k = 1;

-1U:@db_name postgres: SELECT gp_inject_fault('before_xact_end_procarray', 'resume', 1);
1<:
2<:
-1U:@db_name postgres: SELECT gp_inject_fault('all', 'reset', 1);

2: SELECT k, v FROM issue1465_onephase WHERE k = 1;
2: ROLLBACK;

DROP TABLE issue1465_onephase;
