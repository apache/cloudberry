-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--
-- Coverage to ensure that reltuples is updated correctly upon an index build
-- (i.e. CREATE INDEX) on AO/CO tables.
-- FIXME: Currently doesn't assert reltuples on QD (at the moment, we don't
-- aggregate the reltuples counts on QD at end of command)

-- Case 1: Verify that CREATE INDEX is able to update both the aorel's reltuples
-- and the index's reltuples, to equal the actual segment tuple counts.

CREATE TABLE index_build_reltuples_pax(a int);
INSERT INTO index_build_reltuples_pax SELECT generate_series(1, 10);

CREATE INDEX ON index_build_reltuples_pax(a);

SELECT gp_segment_id, count(*) FROM index_build_reltuples_pax
	GROUP BY gp_segment_id ORDER BY gp_segment_id;
SELECT gp_segment_id, reltuples FROM gp_dist_random('pg_class')
	WHERE relname='index_build_reltuples_pax' ORDER BY gp_segment_id;
SELECT gp_segment_id, reltuples FROM gp_dist_random('pg_class')
	WHERE relname='index_build_reltuples_pax_a_idx' ORDER BY gp_segment_id;

DROP TABLE index_build_reltuples_pax;

-- Case 2: Verify that CREATE INDEX is able to update the aorel's reltuples
-- to equal the actual segment tuple counts, when there are deleted tuples. For
-- the index, since we don't have a notion of "recently dead" vs surely dead,
-- we are conservative and form index entries even for deleted tuples. Thus, the
-- reltuples count for the index would also account for deleted tuples.

CREATE TABLE index_build_reltuples_pax(a int);
INSERT INTO index_build_reltuples_pax SELECT generate_series(1, 20);

SELECT gp_segment_id, count(*) FROM index_build_reltuples_pax
	GROUP BY gp_segment_id ORDER BY gp_segment_id;

DELETE FROM index_build_reltuples_pax WHERE a <= 10;

CREATE INDEX ON index_build_reltuples_pax(a);

SELECT gp_segment_id, count(*) FROM index_build_reltuples_pax
	GROUP BY gp_segment_id ORDER BY gp_segment_id;
SELECT gp_segment_id, reltuples FROM gp_dist_random('pg_class')
	WHERE relname='index_build_reltuples_pax' ORDER BY gp_segment_id;
SELECT gp_segment_id, reltuples FROM gp_dist_random('pg_class')
	WHERE relname='index_build_reltuples_pax_a_idx' ORDER BY gp_segment_id;

DROP TABLE index_build_reltuples_pax;

-- Case 3: Verify that CREATE INDEX is able to update both the aorel's reltuples
-- and the index's reltuples, to equal the actual segment tuple counts, when
-- there are aborted tuples.

CREATE TABLE index_build_reltuples_pax(a int);

INSERT INTO index_build_reltuples_pax SELECT generate_series(1, 10);

SELECT gp_segment_id, count(*) FROM index_build_reltuples_pax
	GROUP BY gp_segment_id ORDER BY gp_segment_id;

BEGIN;
INSERT INTO index_build_reltuples_pax SELECT generate_series(11, 20);
ABORT;

CREATE INDEX ON index_build_reltuples_pax(a);

SELECT gp_segment_id, count(*) FROM index_build_reltuples_pax
	GROUP BY gp_segment_id ORDER BY gp_segment_id;
SELECT gp_segment_id, reltuples FROM gp_dist_random('pg_class')
	WHERE relname='index_build_reltuples_pax' ORDER BY gp_segment_id;
SELECT gp_segment_id, reltuples FROM gp_dist_random('pg_class')
	WHERE relname='index_build_reltuples_pax_a_idx' ORDER BY gp_segment_id;

DROP TABLE index_build_reltuples_pax;

-- Case 4: Verify that CREATE INDEX is able to update both the aorel's reltuples
-- and the index's reltuples, to equal the latest segment tuple counts, even
-- when it is executed in a transaction with a snapshot that precedes the INSERT
-- (highlights the need for using SnapshotAny)

CREATE TABLE index_build_reltuples_pax(a int);

1: BEGIN ISOLATION LEVEL REPEATABLE READ;
1: SELECT gp_segment_id, count(*) FROM index_build_reltuples_pax
    GROUP BY gp_segment_id ORDER BY gp_segment_id;

INSERT INTO index_build_reltuples_pax SELECT generate_series(1, 10);

1: CREATE INDEX ON index_build_reltuples_pax(a);
1: COMMIT;

SELECT gp_segment_id, count(*) FROM index_build_reltuples_pax
    GROUP BY gp_segment_id ORDER BY gp_segment_id;
SELECT gp_segment_id, reltuples FROM gp_dist_random('pg_class')
    WHERE relname='index_build_reltuples_pax' ORDER BY gp_segment_id;
SELECT gp_segment_id, reltuples FROM gp_dist_random('pg_class')
    WHERE relname='index_build_reltuples_pax_a_idx' ORDER BY gp_segment_id;

DROP TABLE index_build_reltuples_pax;
