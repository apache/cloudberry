-- install: the extension installs, ships exactly the objects its script
-- declares, and the wrapper carries the mpp_execute default that puts a
-- ForeignScan on every segment.  Every other suite assumes this one ran first.
--
-- Every suite here prints unaligned: the column widths of aligned output carry
-- no information these tests are about, and unaligned output has no trailing
-- whitespace, which keeps the expected files reviewable by eye.
\pset format unaligned
CREATE EXTENSION lance_fdw;
-- CREATE EXTENSION does not load the library; the GUCs appear once it is.
LOAD 'lance_fdw';
SELECT extname, extversion FROM pg_extension WHERE extname = 'lance_fdw';
SELECT pg_describe_object(d.classid, d.objid, 0) AS object
  FROM pg_depend d
  WHERE d.refclassid = 'pg_extension'::regclass
    AND d.refobjid = (SELECT oid FROM pg_extension WHERE extname = 'lance_fdw')
    AND d.deptype = 'e'
  ORDER BY 1;
SELECT fdwname, fdwoptions FROM pg_foreign_data_wrapper WHERE fdwname = 'lance_fdw';
SELECT name, setting FROM pg_settings WHERE name LIKE 'lance\_fdw.%' ORDER BY name;
-- The database's collation is part of the environment these suites assume.
-- Several compare text by ordering, and their expected counts are those of a
-- C.UTF-8 database, which orders by code point; under a linguistic locale the
-- same query returns different rows, correctly.  Checked here, first, so that
-- the wrong locale fails as itself rather than as a count deep in pushdown.
SELECT datcollate, datctype FROM pg_database WHERE datname = current_database();
CREATE SCHEMA lance_feature;
-- Errors that quote a path, a bucket or a lance message are not comparable
-- across environments, so the suites that provoke them run the statement
-- through this and compare the shape of the failure instead of its text.
CREATE FUNCTION lance_feature.capture(stmt text) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE stmt;
  RETURN 'no error';
EXCEPTION WHEN OTHERS THEN
  RETURN CASE
    WHEN SQLERRM LIKE 'lance: %' THEN 'lance error'
    ELSE 'other error: ' || SQLERRM
  END;
END;
$$;
-- Errors raised on a segment arrive with "(segN host:port pid=N)" appended,
-- which is not comparable either.  Suites that want to read the wording of an
-- error - the type checks, whose whole point is what the message says - go
-- through this instead of letting the statement fail.
CREATE FUNCTION lance_feature.message(stmt text) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE stmt;
  RETURN 'no error';
EXCEPTION WHEN OTHERS THEN
  RETURN regexp_replace(SQLERRM, '\s*\(seg\d+ [^)]*\)$', '');
END;
$$;
-- An error whose detail names the uri being read.  For those the detail and the
-- hint are part of what is being checked - which path refused, and what to do
-- about it - but the uri is a path or a bucket and differs per environment.
-- This returns all three parts the way psql would print them, with the uri
-- redacted as explain_lance() redacts it in a plan.
CREATE FUNCTION lance_feature.report(stmt text) RETURNS SETOF text
LANGUAGE plpgsql AS $$
DECLARE
  msg text;
  detail text;
  hint text;
BEGIN
  EXECUTE stmt;
  RETURN NEXT 'no error';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS msg = MESSAGE_TEXT,
                          detail = PG_EXCEPTION_DETAIL,
                          hint = PG_EXCEPTION_HINT;
  RETURN NEXT 'ERROR:  ' || regexp_replace(msg, '\s*\(seg\d+ [^)]*\)$', '');
  IF detail <> '' THEN
    RETURN NEXT 'DETAIL:  ' || regexp_replace(detail, 'uri [^ ]*[^ .]', 'uri <redacted>');
  END IF;
  IF hint <> '' THEN
    RETURN NEXT 'HINT:  ' || hint;
  END IF;
END;
$$;
-- A plan names the resolved uri, which is a path or a bucket and so differs
-- per environment.  Suites that check a plan go through these two instead of
-- printing it: explain_lance() keeps the wrapper's own plan lines with the uri
-- redacted, and plan_mentions() answers whether a literal occurs anywhere in
-- the plan without putting the plan in the expected output.
CREATE FUNCTION lance_feature.explain_lance(stmt text, opts text DEFAULT 'COSTS OFF')
RETURNS SETOF text
LANGUAGE plpgsql AS $$
DECLARE
  line text;
BEGIN
  FOR line IN EXECUTE format('EXPLAIN (%s) %s', opts, stmt)
  LOOP
    line := btrim(line);
    IF line LIKE 'Lance %' THEN
      RETURN NEXT regexp_replace(line, '^Lance URI: .*$', 'Lance URI: <redacted>');
    END IF;
  END LOOP;
END;
$$;
CREATE FUNCTION lance_feature.plan_mentions(stmt text, opts text, needle text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
  line text;
BEGIN
  FOR line IN EXECUTE format('EXPLAIN (%s) %s', opts, stmt)
  LOOP
    IF position(needle IN line) > 0 THEN
      RETURN true;
    END IF;
  END LOOP;
  RETURN false;
END;
$$;
