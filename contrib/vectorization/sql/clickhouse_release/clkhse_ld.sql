
-- load data for hits and hits_tmp tables

\copy hits_tmp from 'data/hits_release.data' WITH (FORMAT csv, DELIMITER ',');

SET vector.enable_vectorization=OFF;

insert into hits select * from hits_tmp;

SET vector.enable_vectorization=ON;