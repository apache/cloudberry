-- The cluster must preload ext_vacuum_statistics on every instance.
SELECT current_setting('vacuum_statistics.enabled') IS NOT NULL AS module_loaded;
