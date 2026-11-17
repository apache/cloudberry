CREATE EXTENSION gp_sparse_vector;
SET search_path = sparse_vector, public;

-- The strict backend cannot install the published 1.0.1 definitions. This
-- fixture reconstructs the catalog state independently observed after a
-- real CREATE EXTENSION VERSION '1.0.1' on the legacy operator backend.
CREATE FUNCTION public.svec_legacy_fixture() RETURNS SETOF boolean
LANGUAGE plpgsql VOLATILE EXECUTE ON COORDINATOR
SET search_path = pg_catalog, pg_temp
SET allow_system_table_mods = true
SET allow_segment_DML = true
AS $$
BEGIN
    UPDATE pg_operator SET
        oprcom = 'sparse_vector.=(sparse_vector.svec,sparse_vector.svec)'::regoperator,
        oprnegate = 'sparse_vector.<>(sparse_vector.svec,sparse_vector.svec)'::regoperator
    WHERE oid IN ('sparse_vector.=(sparse_vector.svec,sparse_vector.svec)'::regoperator,
                  'sparse_vector.==(sparse_vector.svec,sparse_vector.svec)'::regoperator);
    UPDATE pg_operator SET
        oprcom = oid,
        oprnegate = 'sparse_vector.=(sparse_vector.svec,sparse_vector.svec)'::regoperator,
        oprcode = 'sparse_vector.svec_l2_eq(sparse_vector.svec,sparse_vector.svec)'::regprocedure
    WHERE oid = 'sparse_vector.<>(sparse_vector.svec,sparse_vector.svec)'::regoperator;
    UPDATE pg_depend SET
        refobjid = 'sparse_vector.svec_l2_eq(sparse_vector.svec,sparse_vector.svec)'::regprocedure
    WHERE classid = 'pg_operator'::regclass
      AND objid = 'sparse_vector.<>(sparse_vector.svec,sparse_vector.svec)'::regoperator
      AND refclassid = 'pg_proc'::regclass
      AND refobjid = 'sparse_vector.svec_l2_ne(sparse_vector.svec,sparse_vector.svec)'::regprocedure
      AND deptype = 'n';
    UPDATE pg_extension SET extversion = '1.0.1' WHERE extname = 'gp_sparse_vector';
    RETURN NEXT true;
END
$$;
SELECT public.svec_legacy_fixture();
ALTER FUNCTION public.svec_legacy_fixture() EXECUTE ON ALL SEGMENTS;
SELECT public.svec_legacy_fixture();
DROP FUNCTION public.svec_legacy_fixture();

CREATE TABLE upgrade_data(id integer, v svec) DISTRIBUTED BY(id);
INSERT INTO upgrade_data VALUES
    (1, '{1,1}:{3,4}'), (2, '{1,1}:{0,5}'), (3, '{1,1}:{0,6}');
CREATE INDEX upgrade_l2_index ON upgrade_data(v);
CREATE INDEX upgrade_expr_index ON upgrade_data((l2norm(v)));
CREATE VIEW upgrade_safe_view AS SELECT id, v == '{1,1}:{3,4}'::svec AS same_norm
    FROM upgrade_data;
CREATE VIEW upgrade_blocked_view AS SELECT v <> '{1,1}:{3,4}'::svec FROM upgrade_data;
CREATE INDEX upgrade_blocked_index ON upgrade_data((v <> '{1,1}:{3,4}'::svec));

CREATE TEMP TABLE upgrade_before AS
SELECT -1 AS node, oid, oprcom, oprnegate, oprcode::oid AS oprcode
FROM pg_operator WHERE oprnamespace = 'sparse_vector'::regnamespace
UNION ALL
SELECT gp_segment_id, oid, oprcom, oprnegate, oprcode::oid
FROM gp_dist_random('pg_operator') WHERE oprnamespace = 'sparse_vector'::regnamespace
DISTRIBUTED RANDOMLY;
CREATE TEMP TABLE upgrade_object_oids AS
SELECT oid FROM pg_class WHERE relname IN ('upgrade_data', 'upgrade_l2_index',
                                         'upgrade_expr_index', 'upgrade_safe_view')
DISTRIBUTED RANDOMLY;
CREATE TEMP TABLE upgrade_gucs AS
SELECT current_setting('search_path') AS path,
       current_setting('allow_system_table_mods') AS mods,
       current_setting('allow_segment_DML') AS segment_dml
DISTRIBUTED RANDOMLY;

SET plan_cache_mode = force_generic_plan;
PREPARE upgrade_neq(svec,svec) AS SELECT $1 <> $2;
EXECUTE upgrade_neq('{1,1}:{3,4}', '{1,1}:{0,5}');

-- Refusal is atomic; user objects, all four catalogs, and GUCs are unchanged.
ALTER EXTENSION gp_sparse_vector UPDATE TO '1.0.2';
SELECT extversion FROM pg_extension WHERE extname = 'gp_sparse_vector';
SELECT NOT EXISTS (
    SELECT * FROM upgrade_before
    EXCEPT
    (SELECT -1, oid, oprcom, oprnegate, oprcode::oid FROM pg_operator
     WHERE oprnamespace = 'sparse_vector'::regnamespace
     UNION ALL
     SELECT gp_segment_id, oid, oprcom, oprnegate, oprcode::oid
     FROM gp_dist_random('pg_operator')
     WHERE oprnamespace = 'sparse_vector'::regnamespace)) AS catalogs_unchanged;
SELECT to_regclass('upgrade_blocked_view') IS NOT NULL AS view_preserved,
       to_regclass('upgrade_blocked_index') IS NOT NULL AS index_preserved;

-- Explicit preparation of the test-owned dependent objects, not migration DDL.
DROP VIEW upgrade_blocked_view;
DROP INDEX upgrade_blocked_index;
ALTER EXTENSION gp_sparse_vector UPDATE TO '1.0.2';
SELECT extversion FROM pg_extension WHERE extname = 'gp_sparse_vector';
EXECUTE upgrade_neq('{1,1}:{3,4}', '{1,1}:{0,5}');
EXECUTE upgrade_neq('{1,1}:{3,4}', '{1,1}:{0,6}');
SELECT count(*) = 4 AS object_oids_preserved
FROM pg_class JOIN upgrade_object_oids USING (oid);
SELECT * FROM upgrade_safe_view ORDER BY id;
SELECT id, v::float8[] FROM upgrade_data ORDER BY id;
SELECT current_setting('search_path') = path AND
       current_setting('allow_system_table_mods') = mods AND
       current_setting('allow_segment_DML') = segment_dml AS gucs_restored
FROM upgrade_gucs;
SELECT count(*) = 0 AS helper_removed FROM pg_proc WHERE proname = '_upgrade_1_0_2';
SELECT count(*) = 0 AS segment_helpers_removed
FROM gp_dist_random('pg_proc') WHERE proname = '_upgrade_1_0_2';

SELECT count(*) = 12 AND bool_and(oprcom = oid) AS all_self_commutators,
       bool_and(CASE oprname
                    WHEN '=' THEN oprnegate = 0
                    WHEN '==' THEN oprnegate = '<>(svec,svec)'::regoperator
                    WHEN '<>' THEN oprnegate = '==(svec,svec)'::regoperator
                END) AS correct_negators
FROM (SELECT oid, oprname, oprcom, oprnegate FROM pg_operator
      WHERE oprnamespace = 'sparse_vector'::regnamespace
        AND oprname IN ('=', '==', '<>') AND oprleft = 'svec'::regtype
      UNION ALL
      SELECT oid, oprname, oprcom, oprnegate FROM gp_dist_random('pg_operator')
      WHERE oprnamespace = 'sparse_vector'::regnamespace
        AND oprname IN ('=', '==', '<>') AND oprleft = 'svec'::regtype) AS operators;
SELECT count(*) = 4 AS implementation_dependencies
FROM (SELECT refobjid FROM pg_depend
      WHERE classid = 'pg_operator'::regclass
        AND objid = '<>(svec,svec)'::regoperator
        AND refclassid = 'pg_proc'::regclass AND deptype = 'n'
      UNION ALL
      SELECT refobjid FROM gp_dist_random('pg_depend')
      WHERE classid = 'pg_operator'::regclass
        AND objid = '<>(svec,svec)'::regoperator
        AND refclassid = 'pg_proc'::regclass AND deptype = 'n') AS dependencies
WHERE refobjid = 'svec_l2_ne(svec,svec)'::regprocedure;

DEALLOCATE upgrade_neq;
RESET plan_cache_mode;
DROP VIEW upgrade_safe_view;
DROP TABLE upgrade_data;
DROP EXTENSION gp_sparse_vector;
RESET search_path;

-- Fresh-install name collisions must not drop an existing type or its data.
-- DROP EXTENSION leaves the fixed schema in place.
CREATE DOMAIN sparse_vector.svec AS integer;
CREATE TABLE public.upgrade_collision(v sparse_vector.svec) DISTRIBUTED RANDOMLY;
INSERT INTO public.upgrade_collision VALUES (42);
CREATE EXTENSION gp_sparse_vector;
SELECT * FROM public.upgrade_collision;
DROP TABLE public.upgrade_collision;
DROP DOMAIN sparse_vector.svec;
DROP SCHEMA sparse_vector;
