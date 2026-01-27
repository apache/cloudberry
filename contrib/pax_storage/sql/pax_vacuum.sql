show autovacuum;
-- async collect stats in vacuum
set pax.enable_sync_collect_stats to off;
create table pax_vacuum (a int, b int, c int) using pax with(minmax_columns='a,b') distributed by (a);
insert into pax_vacuum select i, i from generate_series(1, 1000) i;
select * from get_pax_aux_table('pax_vacuum');
vacuum pax_vacuum;
select * from get_pax_aux_table('pax_vacuum');
truncate pax_vacuum;
select * from get_pax_aux_table('pax_vacuum');

-- sync collect stats in vacuum
set pax.enable_sync_collect_stats to on;
insert into pax_vacuum select i, i from generate_series(1, 1000) i;
select * from get_pax_aux_table('pax_vacuum');
vacuum pax_vacuum;
select * from get_pax_aux_table('pax_vacuum');
truncate pax_vacuum;
select * from get_pax_aux_table('pax_vacuum');

-- update and vacuum
set pax.enable_sync_collect_stats to off;
insert into pax_vacuum select i, i from generate_series(1, 1000) i;
select * from get_pax_aux_table('pax_vacuum');
vacuum pax_vacuum;
select * from get_pax_aux_table('pax_vacuum');
update pax_vacuum set b = b + 1;
select * from get_pax_aux_table('pax_vacuum');
vacuum pax_vacuum;
select * from get_pax_aux_table('pax_vacuum');
truncate pax_vacuum;
select * from get_pax_aux_table('pax_vacuum');

-- sync collect stats with vacuum
set pax.enable_sync_collect_stats to on;
insert into pax_vacuum select i, i from generate_series(1, 1000) i;
select * from get_pax_aux_table('pax_vacuum');
vacuum pax_vacuum;
select * from get_pax_aux_table('pax_vacuum');
update pax_vacuum set b = b + 1;
select * from get_pax_aux_table('pax_vacuum');
vacuum pax_vacuum;
select * from get_pax_aux_table('pax_vacuum');
truncate pax_vacuum;
select * from get_pax_aux_table('pax_vacuum');


drop table pax_vacuum;