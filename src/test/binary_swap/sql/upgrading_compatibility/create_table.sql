-- Create various types of tables for migration compatibility test
--
--                            List of tables in Summary
--            Name            |       Type        | Partition by |       Storage        
------------------------------+-------------------+--------------+----------------------
-- sales_ao                   | table             |              | append only
-- sales_aocs                 | table             |              | append only columnar
-- sales_heap                 | table             |              | heap
-- sales_partition_ao         | partitioned table |    range     | 
-- sales_partition_aocs       | partitioned table |    hash      | 
-- sales_partition_heap       | partitioned table |    list      | 
-- sales_partition_ao_part1   | table             |              | append only
-- sales_partition_ao_part2   | table             |              | append only
-- sales_partition_ao_part3   | table             |              | append only
-- sales_partition_aocs_part1 | table             |              | append only columnar
-- sales_partition_aocs_part2 | table             |              | append only columnar
-- sales_partition_aocs_part3 | table             |              | append only columnar
-- sales_partition_heap_part1 | table             |              | heap
-- sales_partition_heap_part2 | table             |              | heap
-- sales_partition_heap_part3 | table             |              | heap
--
-- Heap Table
DROP TABLE IF EXISTS sales_heap; 
CREATE TABLE IF NOT EXISTS sales_heap
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
DISTRIBUTED BY (product_id);

-- Heap Table, Partition by LIST
DROP TABLE IF EXISTS sales_partition_heap;
CREATE TABLE IF NOT EXISTS sales_partition_heap
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
PARTITION BY LIST (status);

CREATE TABLE sales_partition_heap_part1
PARTITION OF sales_partition_heap
FOR VALUES IN ('Cancelled');

CREATE TABLE sales_partition_heap_part2
PARTITION OF sales_partition_heap
FOR VALUES IN ('Closed');

CREATE TABLE sales_partition_heap_part3
PARTITION OF sales_partition_heap
FOR VALUES IN ('Processing');

-- AO Table
DROP TABLE IF EXISTS sales_ao;
CREATE TABLE IF NOT EXISTS sales_ao
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
WITH (appendonly=true)
DISTRIBUTED BY (product_id);

-- AO Table, Partition by Range
DROP TABLE IF EXISTS sales_partition_ao;
CREATE TABLE IF NOT EXISTS sales_partition_ao
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
PARTITION BY RANGE (order_date);

CREATE TABLE sales_partition_ao_part1
PARTITION OF sales_partition_ao
FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')
WITH (appendonly=true);

CREATE TABLE sales_partition_ao_part2
PARTITION OF sales_partition_ao
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')
WITH (appendonly=true);

CREATE TABLE sales_partition_ao_part3
PARTITION OF sales_partition_ao
FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')
WITH (appendonly=true);

-- AOCS Table
DROP TABLE IF EXISTS sales_aocs;
CREATE TABLE IF NOT EXISTS sales_aocs
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
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (product_id);

-- AOCS Table, Partition by Hash
DROP TABLE IF EXISTS sales_partition_aocs;
CREATE TABLE IF NOT EXISTS sales_partition_aocs
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

CREATE TABLE sales_partition_aocs_part1
PARTITION OF sales_partition_aocs
FOR VALUES WITH (MODULUS 3, REMAINDER 0)
WITH (appendonly=true, orientation=column);

CREATE TABLE sales_partition_aocs_part2
PARTITION OF sales_partition_aocs
FOR VALUES WITH (MODULUS 3, REMAINDER 1)
WITH (appendonly=true, orientation=column);

CREATE TABLE sales_partition_aocs_part3
PARTITION OF sales_partition_aocs
FOR VALUES WITH (MODULUS 3, REMAINDER 2)
WITH (appendonly=true, orientation=column);