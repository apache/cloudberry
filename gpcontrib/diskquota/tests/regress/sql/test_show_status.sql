select * from diskquota.status();

select from diskquota.enable_hardlimit();
select * from diskquota.status();

select from diskquota.disable_hardlimit();
select * from diskquota.status();

select from diskquota.pause();
select * from diskquota.status();

select from diskquota.enable_hardlimit();
select * from diskquota.status();

select from diskquota.disable_hardlimit();
select * from diskquota.status();

select from diskquota.resume();
select from diskquota.disable_hardlimit();
select * from diskquota.status();
