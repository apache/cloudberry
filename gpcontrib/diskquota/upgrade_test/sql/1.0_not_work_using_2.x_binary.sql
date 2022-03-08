-- cleanup previous diskquota installation
\! gpconfig -c shared_preload_libraries -v ''
\! gpstop -raf > /dev/null
drop database if exists diskquota;

-- TODO reset all diskquota GUC
\! gpstop -raf > /dev/null

-- setup basic environment
create database diskquota;

\! gpconfig -c shared_preload_libraries -v 'diskquota-1.0.so'
\! gpconfig -c diskquota.naptime -v '1'
\! gpstop -raf > /dev/null

create extension diskquota version '1.0' -- for now 1.o installed

\! gpconfig -c shared_preload_libraries -v 'diskquota-2.0.so'
\! gpstop -raf > /dev/null

-- FIXME check diskquota shoud prompt user to do upgrade
