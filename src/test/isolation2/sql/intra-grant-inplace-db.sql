-- GRANT's lock is the catalog tuple xmax.  GRANT doesn't acquire a heavyweight
-- lock on the object undergoing an ACL change.  In-place updates, namely
-- datfrozenxid, need special code to cope.

CREATE ROLE regress_temp_grantee;

--start_ignore
DROP TABLE IF EXISTS frozen_witness;
--end_ignore

3: CREATE TEMPORARY TABLE frozen_witness (x xid) distributed by (x);
-- observe datfrozenxid
3: INSERT INTO frozen_witness SELECT datfrozenxid FROM pg_database WHERE datname = current_catalog;
1: BEGIN;
-- heap_update(pg_database)
1: GRANT TEMP ON DATABASE isolation2test TO regress_temp_grantee;
-- inplace update
2&: VACUUM (FREEZE);
3: INSERT INTO frozen_witness SELECT datfrozenxid FROM pg_database WHERE datname = current_catalog;
1: COMMIT;
2<:
-- Save the result in an environment variable.
-- We get the raw xid to be sure that the age in the next query will be executed on the coordinator.
3: @post_run 'TOKEN=`echo "${RAW_STR}" | awk \'NR==3\' | awk \'{print $1}\'` && echo ""' : SELECT min(x::varchar::int) FROM frozen_witness;
-- observe datfrozenxid
3: @pre_run 'echo "${RAW_STR}" | sed "s#@TOKEN#${TOKEN}#"': SELECT 'datfrozenxid retreated' FROM pg_database WHERE datname = current_catalog AND age(datfrozenxid) > age('@TOKEN'::xid);

REVOKE ALL ON DATABASE isolation2test FROM regress_temp_grantee;
DROP ROLE regress_temp_grantee;

1q:
2q:
3q:
