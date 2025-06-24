----------------------------------------------------------------
-- Test guc gp_hot_standby_snapshot_restore_point_name
-- when the guc set, for example rp1, the hot_standby query only can see
-- the data commited before rp1.
----------------------------------------------------------------
!\retcode gpconfig -c gp_hot_standby_snapshot_restore_point_name -v "rp1";
!\retcode gpstop -ar;

1: show gp_hot_standby_snapshot_restore_point_name;
1: create table hs_sh(a int);
1: insert into hs_sh select * from generate_series(1,10);

-1S: select * from hs_sh;;

1: select gp_create_restore_point('rp1');

-1S: select * from hs_sh;



