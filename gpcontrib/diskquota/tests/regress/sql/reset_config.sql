--start_ignore
\! gpconfig -c diskquota.naptime -v 2
\! gpstop -u
--end_ignore

SHOW diskquota.naptime;
