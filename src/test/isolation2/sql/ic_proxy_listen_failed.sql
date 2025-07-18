-- Test case for the scenario which ic-proxy peer listener port has been occupied

-- start_matchsubs
-- m/ic_tcp.c:\d+/
-- s/ic_tcp.c:\d+/ic_tcp.c:LINE/
-- end_matchsubs

1:create table PR_16438 (i int);
1:insert into PR_16438 select generate_series(1,100);
1q:

-- get one port and occupy it (start_py_httpserver.sh), then restart cluster
!\retcode ic_proxy_port=`psql postgres -Atc "show gp_interconnect_proxy_addresses;" | awk -F ',' '{print $1}' | awk -F ':' '{print $4}'` && gpstop -ai > /dev/null && ./script/start_py_httpserver.sh $ic_proxy_port;
!\retcode sleep 2 && gpstart -a > /dev/null;

-- this output is hard to match, let's ignore it
-- start_ignore
2&:select count(*) from PR_16438;
2<:
2q:
-- end_ignore

-- execute a query (should failed)
3:select count(*) from PR_16438;

-- kill the script to release port and execute query again (should successfully)
-- Note: different from 7x here, we have to restart cluster (no need in 7x)
-- because 6x's icproxy code doesn't align with 7x: https://github.com/GreengageDB/greengage/issues/14485
!\retcode ps aux | grep SimpleHTTPServer | grep -v grep | awk '{print $2}' | xargs kill;
!\retcode sleep 2 && gpstop -ari > /dev/null;

4:select count(*) from PR_16438;
4:drop table PR_16438;
