alter system set autovacuum = off;
select gp_segment_id, pg_reload_conf() from gp_id union select gp_segment_id, pg_reload_conf() from gp_dist_random('gp_id');
-- start_ignore 
-- The reason for restarting cbdb here is that if the subsequent 
-- vacuum ao test(uao*_catalog_tables/threshold) encounters an
-- unfinished transaction, it will fail.
-- Use fast mode: smart mode would wait forever for this very session to
-- disconnect, while this session is itself waiting for gpstop to return.
\!gpstop -ari
-- end_ignore