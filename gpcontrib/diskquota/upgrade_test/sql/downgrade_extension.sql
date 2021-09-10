\set old_version `echo $OLD_VERSION`
alter extension diskquota update to :'old_version';
