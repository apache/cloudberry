\! gpconfig -c shared_preload_libraries -v 'diskquota-2.3.so' > /dev/null
\! gpstop -raf > /dev/null

\! gpconfig -s 'shared_preload_libraries'

\c
alter extension diskquota update to '2.3';
\! sleep 5
