CREATE FUNCTION typeid_to_name(oid[]) RETURNS name[] AS '
  WITH io AS (
    SELECT x.i AS index, x.o AS type_id FROM (
      SELECT generate_series(1, array_length($1, 1)) AS i, unnest($1) AS o
    ) AS x
  ) SELECT array_agg(typname order by io.index) FROM io, pg_type t WHERE io.type_id = t.oid;
' LANGUAGE sql STABLE;

-- types
SELECT
    t1.typname,
    array_agg(t2.typname order by a.atttypid) typname
FROM
    pg_namespace n,
    pg_class c,
    pg_type t1,
    pg_type t2,
    pg_attribute a
WHERE
    n.nspname = 'diskquota'
    AND c.oid = t1.typrelid
    AND n.oid = t1.typnamespace
    AND a.attrelid = c.oid
    AND t2.oid = a.atttypid
GROUP BY
    t1.typname
ORDER BY
    t1.typname;
-- types end

-- tables
SELECT
    relname,
    typeid_to_name(ARRAY[c.reltype]::oid[]) AS reltype,
    typeid_to_name(ARRAY[c.reloftype]::oid[]) AS reloftype
FROM
    pg_class c,
    pg_namespace n
WHERE
    c.relnamespace = n.oid
    AND n.nspname = 'diskquota'
    and c.relkind != 'v'
ORDER BY
   relname;
-- tables end

-- UDF
SELECT
    proname,
    typeid_to_name(ARRAY[prorettype]::oid[]) AS prorettype,
    typeid_to_name(proargtypes) AS proargtypes,
    typeid_to_name(proallargtypes) AS proallargtypes,
    proargmodes,
    prosrc,
    probin,
    proacl
FROM
    pg_namespace n,
    pg_proc p
WHERE
    n.nspname = 'diskquota'
    AND n.oid = p.pronamespace
ORDER BY
    proname;
-- UDF end

-- views
SELECT
    schemaname,
    viewname,
    definition
FROM
    pg_views
WHERE
    schemaname = 'diskquota'
ORDER BY
    schemaname, viewname;
-- views end

DROP FUNCTION typeid_to_name(oid[]);
