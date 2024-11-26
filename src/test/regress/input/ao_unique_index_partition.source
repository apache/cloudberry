--
-- Github issue: https://github.com/apache/cloudberry/issues/557
-- Test update on ao partition tables with unique index.
--
create schema t_issue_557;
set search_path to t_issue_557;
CREATE TABLE IF NOT EXISTS t_issue_557_ao
(
product_id INT,
is_audited BOOLEAN DEFAULT FALSE,
quantity SMALLINT,
total_sales BIGINT,
unit_price REAL,
discount DOUBLE PRECISION,
description TEXT,
sale_date TIMESTAMP,
order_date DATE,
status CHAR(10),
customer_name VARCHAR(20),
price DECIMAL(20, 10)
)
DISTRIBUTED BY (product_id)
PARTITION BY HASH(description);

CREATE TABLE t_issue_557_ao_part1
PARTITION OF t_issue_557_ao
FOR VALUES WITH (MODULUS 3, REMAINDER 0)
WITH (appendonly=true);

CREATE TABLE t_issue_557_ao_part2
PARTITION OF t_issue_557_ao
FOR VALUES WITH (MODULUS 3, REMAINDER 1)
WITH (appendonly=true);

CREATE TABLE t_issue_557_ao_part3
PARTITION OF t_issue_557_ao
FOR VALUES WITH (MODULUS 3, REMAINDER 2)
WITH (appendonly=true);
-- Create Indexes

-- Unique
CREATE UNIQUE INDEX on t_issue_557_ao(product_id,description);

INSERT INTO t_issue_557_ao (
product_id,
is_audited,
description,
status
)
SELECT
x.id, -- product_id
FALSE,
'Product description ' || x.id, -- description
'Closed'
FROM (
SELECT * FROM generate_series(1, 20) AS id
) AS x;

UPDATE t_issue_557_ao
SET status = 'Closed',
description = description || ' Audited';

DELETE FROM t_issue_557_ao;

INSERT INTO t_issue_557_ao (
product_id,
is_audited,
description,
status
)
SELECT
x.id, -- product_id
FALSE,
'Product description ' || x.id, -- description
'Closed'
FROM (
SELECT * FROM generate_series(1, 20) AS id
) AS x;

UPDATE t_issue_557_ao
SET status = 'Closed',
description = description || ' Audited';

-- AOCO
CREATE TABLE IF NOT EXISTS t_issue_557_aocs
(
product_id INT,
is_audited BOOLEAN DEFAULT FALSE,
quantity SMALLINT,
total_sales BIGINT,
unit_price REAL,
discount DOUBLE PRECISION,
description TEXT,
sale_date TIMESTAMP,
order_date DATE,
status CHAR(10),
customer_name VARCHAR(20),
price DECIMAL(20, 10)
)
DISTRIBUTED BY (product_id)
PARTITION BY HASH(description);

CREATE TABLE t_issue_557_aocs_part1
PARTITION OF t_issue_557_aocs
FOR VALUES WITH (MODULUS 3, REMAINDER 0)
WITH (appendonly=true, orientation=column);

CREATE TABLE t_issue_557_aocs_part2
PARTITION OF t_issue_557_aocs
FOR VALUES WITH (MODULUS 3, REMAINDER 1)
WITH (appendonly=true, orientation=column);

CREATE TABLE t_issue_557_aocs_part3
PARTITION OF t_issue_557_aocs
FOR VALUES WITH (MODULUS 3, REMAINDER 2)
WITH (appendonly=true, orientation=column);
-- Create Indexes

-- Unique
CREATE UNIQUE INDEX on t_issue_557_aocs(product_id,description);

INSERT INTO t_issue_557_aocs (
product_id,
is_audited,
description,
status
)
SELECT
x.id, -- product_id
FALSE,
'Product description ' || x.id, -- description
'Closed'
FROM (
SELECT * FROM generate_series(1, 20) AS id
) AS x;

UPDATE t_issue_557_aocs
SET status = 'Closed',
description = description || ' Audited';

DELETE FROM t_issue_557_aocs;

INSERT INTO t_issue_557_aocs (
product_id,
is_audited,
description,
status
)
SELECT
x.id, -- product_id
FALSE,
'Product description ' || x.id, -- description
'Closed'
FROM (
SELECT * FROM generate_series(1, 20) AS id
) AS x;

UPDATE t_issue_557_aocs
SET status = 'Closed',
description = description || ' Audited';

-- start_ignore
drop schema t_issue_557 cascade;
-- end_ignore
