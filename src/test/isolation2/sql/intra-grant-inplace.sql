-- GRANT's lock is the catalog tuple xmax.  GRANT doesn't acquire a heavyweight
-- lock on the object undergoing an ACL change.  Inplace updates, such as
-- relhasindex=true, need special code to cope.

CREATE TABLE intra_grant_inplace (c int);

-- Disabling ORCA because we need to use row-level locks.
1: set optimizer = off;
2: set optimizer = off;
3: set optimizer = off;
4: set optimizer = off;
5: set optimizer = off;

1: BEGIN;
1: GRANT SELECT ON intra_grant_inplace TO PUBLIC;

2: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass;
-- inplace waits
2&: ALTER TABLE intra_grant_inplace ADD PRIMARY KEY (c);

1: COMMIT;
2<:
2: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass;

DROP TABLE intra_grant_inplace;
CREATE TABLE intra_grant_inplace (c int);

-- inplace through KEY SHARE
5: BEGIN;
5: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass FOR KEY SHARE;
2: ALTER TABLE intra_grant_inplace ADD PRIMARY KEY (c);
5: ROLLBACK;

DROP TABLE intra_grant_inplace;
CREATE TABLE intra_grant_inplace (c int);

-- inplace wait NO KEY UPDATE w/ KEY SHARE
5: BEGIN;
3: BEGIN ISOLATION LEVEL READ COMMITTED;
3: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass FOR NO KEY UPDATE;
2&: ALTER TABLE intra_grant_inplace ADD PRIMARY KEY (c);
3: ROLLBACK;
5: ROLLBACK;
2<:

DROP TABLE intra_grant_inplace;
CREATE TABLE intra_grant_inplace (c int);

-- same-xact rowmark
2: BEGIN;
2: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass FOR NO KEY UPDATE;
2: ALTER TABLE intra_grant_inplace ADD PRIMARY KEY (c);
2: COMMIT;

DROP TABLE intra_grant_inplace;
CREATE TABLE intra_grant_inplace (c int);

-- same-xact rowmark in multixact
5: BEGIN;
5: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass FOR KEY SHARE;
2: BEGIN;
2: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass FOR NO KEY UPDATE;
2: ALTER TABLE intra_grant_inplace ADD PRIMARY KEY (c);
2: COMMIT;
5: ROLLBACK;

DROP TABLE intra_grant_inplace;
CREATE TABLE intra_grant_inplace (c int);

3: BEGIN ISOLATION LEVEL READ COMMITTED;
3: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass FOR UPDATE;
1: BEGIN;
-- acquire LockTuple(), await session 3 xmax
1&: GRANT SELECT ON intra_grant_inplace TO PUBLIC;
2: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass;
-- block in LockTuple() behind grant1
2&: ALTER TABLE intra_grant_inplace ADD PRIMARY KEY (c);
-- unblock grant1; addk2 now awaits grant1 xmax
3: ROLLBACK;
1<:
1: COMMIT;
2<:
2: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass;

DROP TABLE intra_grant_inplace;
CREATE TABLE intra_grant_inplace (c int);

2: BEGIN;
2: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass FOR NO KEY UPDATE;
1: BEGIN;
-- acquire LockTuple(), await session 2 xmax
1&: GRANT SELECT ON intra_grant_inplace TO PUBLIC;
-- block in LockTuple() behind grant1 = deadlock
2: ALTER TABLE intra_grant_inplace ADD PRIMARY KEY (c);
2: COMMIT;
1<:
1: COMMIT;
2: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass;

DROP TABLE intra_grant_inplace;
CREATE TABLE intra_grant_inplace (c int);

-- SearchSysCacheLocked1() calling LockRelease()
1: BEGIN;
1: GRANT SELECT ON intra_grant_inplace TO PUBLIC;
3: BEGIN ISOLATION LEVEL READ COMMITTED;
3&: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass FOR UPDATE;
4&: DO $$ BEGIN REVOKE SELECT ON intra_grant_inplace FROM PUBLIC; EXCEPTION WHEN others THEN RAISE WARNING 'got: %', regexp_replace(sqlerrm, '[0-9]+', 'REDACTED'); END $$;
1: COMMIT;
3<:
3: ROLLBACK;
4<:

DROP TABLE intra_grant_inplace;
CREATE TABLE intra_grant_inplace (c int);

-- SearchSysCacheLocked1() finding a tuple, then no tuple
1: BEGIN;
1: DROP TABLE intra_grant_inplace;
3: BEGIN ISOLATION LEVEL READ COMMITTED;
3&: SELECT relhasindex FROM pg_class WHERE oid = 'intra_grant_inplace'::regclass FOR UPDATE;
4&: DO $$ BEGIN REVOKE SELECT ON intra_grant_inplace FROM PUBLIC; EXCEPTION WHEN others THEN RAISE WARNING 'got: %', regexp_replace(sqlerrm, '[0-9]+', 'REDACTED'); END $$;
1: COMMIT;
3<:
3: ROLLBACK;
4<:

1q:
2q:
3q:
4q:
5q:
