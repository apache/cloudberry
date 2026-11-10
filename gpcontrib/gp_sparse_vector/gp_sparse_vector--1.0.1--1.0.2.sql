-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gp_sparse_vector UPDATE TO '1.0.2'" to load this file. \quit

-- ALTER OPERATOR cannot change these attributes. Keep the operator OIDs and
-- the L2 btree operator class, but do not silently rewrite user expressions:
-- stored expression trees cache the old implementation function of <>.
CREATE FUNCTION sparse_vector._upgrade_1_0_2() RETURNS SETOF boolean
LANGUAGE plpgsql VOLATILE EXECUTE ON COORDINATOR
SET search_path = pg_catalog, pg_temp
SET allow_system_table_mods = true
SET allow_segment_DML = true
AS $$
DECLARE
    unequal oid := 'sparse_vector.<>(sparse_vector.svec,sparse_vector.svec)'::regoperator;
    old_function oid := 'sparse_vector.svec_l2_eq(sparse_vector.svec,sparse_vector.svec)'::regprocedure;
    new_function oid := 'sparse_vector.svec_l2_ne(sparse_vector.svec,sparse_vector.svec)'::regprocedure;
    dependencies text;
    old_dependencies integer;
    new_dependencies integer;
    updated_dependencies integer;
BEGIN
    -- Serialize dependency catalog writes until this transaction finishes.
    -- Run without concurrent schema DDL: a command parsed before this lock
    -- could otherwise retain the old implementation function in its tree.
    LOCK TABLE pg_catalog.pg_depend IN SHARE ROW EXCLUSIVE MODE;

    SELECT string_agg(description, E'\n' ORDER BY description)
    INTO dependencies
    FROM (SELECT DISTINCT coalesce(pg_catalog.pg_describe_object(classid, objid, objsubid),
                                   format('catalog %s object %s subobject %s',
                                          classid, objid, objsubid)) AS description
          FROM pg_catalog.pg_depend
          WHERE refclassid = 'pg_catalog.pg_operator'::regclass
            AND refobjid = unequal) AS dependents;

    IF dependencies IS NOT NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'cannot update gp_sparse_vector while objects depend on operator <>',
            DETAIL = dependencies,
            HINT = 'Save the dependent definitions and prepare these objects for the corrected L2 inequality semantics before retrying ALTER EXTENSION UPDATE. See gp_sparse_vector/README.update.';
    END IF;

    SELECT count(*) FILTER (WHERE refobjid = old_function),
           count(*) FILTER (WHERE refobjid = new_function)
    INTO old_dependencies, new_dependencies
    FROM pg_catalog.pg_depend
    WHERE classid = 'pg_catalog.pg_operator'::regclass
      AND objid = unequal
      AND refclassid = 'pg_catalog.pg_proc'::regclass
      AND deptype = 'n';

    IF old_dependencies + new_dependencies <> 1 THEN
        RAISE EXCEPTION 'unexpected implementation dependencies for sparse_vector.<>';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_catalog.pg_operator
                   WHERE oid = unequal AND oprcode IN (old_function, new_function)) THEN
        RAISE EXCEPTION 'unexpected implementation function for sparse_vector.<>';
    END IF;

    UPDATE pg_catalog.pg_operator
    SET oprcom = oid, oprnegate = 0
    WHERE oid = 'sparse_vector.=(sparse_vector.svec,sparse_vector.svec)'::regoperator;

    UPDATE pg_catalog.pg_operator
    SET oprcom = oid,
        oprnegate = 'sparse_vector.<>(sparse_vector.svec,sparse_vector.svec)'::regoperator
    WHERE oid = 'sparse_vector.==(sparse_vector.svec,sparse_vector.svec)'::regoperator;

    UPDATE pg_catalog.pg_operator
    SET oprcom = oid,
        oprnegate = 'sparse_vector.==(sparse_vector.svec,sparse_vector.svec)'::regoperator,
        oprcode = 'sparse_vector.svec_l2_ne(sparse_vector.svec,sparse_vector.svec)'::regprocedure
    WHERE oid = unequal;

    UPDATE pg_catalog.pg_depend
    SET refobjid = new_function
    WHERE classid = 'pg_catalog.pg_operator'::regclass
      AND objid = unequal
      AND refclassid = 'pg_catalog.pg_proc'::regclass
      AND refobjid = old_function
      AND deptype = 'n';

    GET DIAGNOSTICS updated_dependencies = ROW_COUNT;
    IF updated_dependencies <> old_dependencies THEN
        RAISE EXCEPTION 'unexpected implementation dependency update for sparse_vector.<>';
    END IF;
    RETURN NEXT true;
END
$$;

SELECT sparse_vector._upgrade_1_0_2();
ALTER FUNCTION sparse_vector._upgrade_1_0_2() EXECUTE ON ALL SEGMENTS;
SELECT sparse_vector._upgrade_1_0_2();
DROP FUNCTION sparse_vector._upgrade_1_0_2();
