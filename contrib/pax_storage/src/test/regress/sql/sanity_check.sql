VACUUM;

--
-- sanity check, if we don't have indices the test will take years to
-- complete.  But skip TOAST relations (since they will have varying
-- names depending on the current OID counter) as well as temp tables
-- of other backends (to avoid timing-dependent behavior).
--

-- temporarily disable fancy output, so catalog changes create less diff noise
\a\t

-- filter tables with same name under schema singleseg created by GPDB 
SELECT relname, relhasindex
   FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = relnamespace
   WHERE relkind IN ('r', 'p') AND (nspname ~ '^pg_temp_') IS NOT TRUE
   AND relname NOT LIKE 'gp_%'
   AND relname NOT LIKE '__gp_%'
   -- CBDB: ignore relations in extension namespace
   AND nspname <> 'pg_ext_aux'
   AND relname <> 'pg_resqueue'
   AND n.nspname <> 'singleseg'
   ORDER BY relname;

-- restore normal output mode
\a\t

--
-- another sanity check: every system catalog that has OIDs should have
-- a unique index on OID.  This ensures that the OIDs will be unique,
-- even after the OID counter wraps around.
-- We exclude non-system tables from the check by looking at nspname.
--
SELECT relname, nspname
 FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = relnamespace JOIN pg_attribute a ON (attrelid = c.oid AND attname = 'oid')
 WHERE relkind = 'r' and c.oid < 16384
     AND ((nspname ~ '^pg_') IS NOT FALSE)
     AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE indrelid = c.oid
                     AND indkey[0] = a.attnum AND indnatts = 1
                     AND indisunique AND indimmediate);

-- check that relations without storage don't have relfilenode
SELECT relname, relkind
  FROM pg_class
 WHERE relkind IN ('v', 'c', 'f', 'p', 'I')
       AND relfilenode <> 0;

--
-- When ALIGNOF_DOUBLE==4 (e.g. AIX), the C ABI may impose 8-byte alignment on
-- some of the C types that correspond to TYPALIGN_DOUBLE SQL types.  To ensure
-- catalog C struct layout matches catalog tuple layout, arrange for the tuple
-- offset of each fixed-width, attalign='d' catalog column to be divisible by 8
-- unconditionally.  Keep such columns before the first NameData column of the
-- catalog, since packagers can override NAMEDATALEN to an odd number.
--
WITH check_columns AS (
 SELECT relname, attname,
  array(
   SELECT t.oid
    FROM pg_type t JOIN pg_attribute pa ON t.oid = pa.atttypid
    WHERE pa.attrelid = a.attrelid AND
          pa.attnum > 0 AND pa.attnum < a.attnum
    ORDER BY pa.attnum) AS coltypes
 FROM pg_attribute a JOIN pg_class c ON c.oid = attrelid
  JOIN pg_namespace n ON c.relnamespace = n.oid
 WHERE attalign = 'd' AND relkind = 'r' AND
  attnotnull AND attlen <> -1 AND n.nspname = 'pg_catalog'
)
SELECT relname, attname, coltypes, get_columns_length(coltypes)
 FROM check_columns
 WHERE get_columns_length(coltypes) % 8 != 0 OR
       'name'::regtype::oid = ANY(coltypes);
