-- explain: what a plan says about a Lance scan, and what it costs to ask.
--
-- EXPLAIN without ANALYZE opens nothing, so it knows the uri and the requested
-- version but not the fragment count - and it works against a server whose
-- credentials would fail.  EXPLAIN ANALYZE has really run, so the QD has
-- listed the fragments and can say how many there were.
\pset format unaligned
DO $$
BEGIN
  EXECUTE format('CREATE SERVER expl_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
END
$$;
CREATE FOREIGN TABLE lance_feature.expl_frag3 (id integer, v text, n bigint)
  SERVER expl_files OPTIONS (uri 'frag_3.lance');
SELECT * FROM lance_feature.explain_lance('SELECT * FROM lance_feature.expl_frag3');
SELECT * FROM lance_feature.explain_lance('SELECT id FROM lance_feature.expl_frag3 WHERE n > 4',
                                          'VERBOSE, COSTS OFF');
SELECT * FROM lance_feature.explain_lance('SELECT * FROM lance_feature.expl_frag3',
                                          'ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF');
-- The scan still runs on every segment, which is what puts a Gather Motion
-- above it.
SELECT lance_feature.plan_mentions('SELECT * FROM lance_feature.expl_frag3',
                                   'COSTS OFF', 'Gather Motion') AS gather_motion;
SELECT lance_feature.plan_mentions('SELECT * FROM lance_feature.expl_frag3',
                                   'COSTS OFF', 'Foreign Scan') AS foreign_scan;
-- A plain EXPLAIN does no I/O, so a server nothing could authenticate against
-- still produces a plan.
DO $$
BEGIN
  EXECUTE format('CREATE SERVER expl_badcreds FOREIGN DATA WRAPPER lance_fdw '
                 'OPTIONS (base_uri %L, aws_endpoint %L, aws_region %L, allow_http %L)',
                 's3://' || current_setting('regress.s3_bucket') || '/fixtures',
                 current_setting('regress.s3_endpoint'),
                 current_setting('regress.s3_region'), 'true');
  EXECUTE 'CREATE USER MAPPING FOR PUBLIC SERVER expl_badcreds '
          'OPTIONS (aws_access_key_id ''wrong-key'', aws_secret_access_key ''wrong-secret'')';
END
$$;
CREATE FOREIGN TABLE lance_feature.expl_badcreds_t (id integer, v text, n bigint)
  SERVER expl_badcreds OPTIONS (uri 'frag_3.lance', version '7');
SELECT * FROM lance_feature.explain_lance('SELECT * FROM lance_feature.expl_badcreds_t');
-- A uri that does not exist is equally uninteresting to a plain EXPLAIN.
CREATE FOREIGN TABLE lance_feature.expl_missing (id integer)
  SERVER expl_files OPTIONS (uri 'no_such_dataset.lance');
SELECT * FROM lance_feature.explain_lance('SELECT * FROM lance_feature.expl_missing');
