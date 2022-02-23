--start_ignore
\! gpconfig -c diskquota.naptime -v 10
\! gpstop -u
--end_ignore

SHOW diskquota.naptime;
