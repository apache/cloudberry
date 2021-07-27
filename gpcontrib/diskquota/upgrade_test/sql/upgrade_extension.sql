\set new_version `echo $NEW_VERSION`
alter extension diskquota update to :'new_version';
