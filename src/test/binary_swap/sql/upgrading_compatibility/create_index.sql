-- Create Indexes

-- B-tree
CREATE INDEX btree_idx_sales_heap_1 ON sales_heap(product_id);
CREATE INDEX btree_idx_sales_heap_2 ON sales_heap(customer_name);
CREATE INDEX btree_idx_sales_ao_1 ON sales_ao(product_id);
CREATE INDEX btree_idx_sales_ao_2 ON sales_ao(customer_name);
CREATE INDEX btree_idx_sales_aocs_1 ON sales_aocs(product_id);
CREATE INDEX btree_idx_sales_aocs_2 ON sales_aocs(product_id, sale_date);
CREATE INDEX btree_idx_sales_partition_heap_1 ON sales_partition_heap(product_id);
CREATE INDEX btree_idx_sales_partition_heap_2 ON sales_partition_heap(customer_name);
CREATE INDEX btree_idx_sales_partition_ao_1 ON sales_partition_ao(product_id);
CREATE INDEX btree_idx_sales_partition_ao_2 ON sales_partition_ao(product_id, sale_date);
CREATE INDEX btree_idx_sales_partition_ao_3 ON sales_partition_ao(customer_name);
CREATE INDEX btree_idx_sales_partition_aocs1 ON sales_partition_aocs(product_id);
CREATE INDEX btree_idx_sales_partition_aocs2 ON sales_partition_aocs(customer_name, product_id);

-- Unique
CREATE UNIQUE INDEX on sales_ao(product_id);
CREATE UNIQUE INDEX on sales_aocs(product_id,description);
CREATE UNIQUE INDEX on sales_partition_ao(product_id,order_date);
-- CREATE UNIQUE INDEX on sales_partition_aocs(product_id,description);  blocked by issue #557

-- Bitmap 
CREATE INDEX bmp_idx_sales_ao ON sales_ao USING bitmap(is_audited);
CREATE INDEX bmp_idx_sales_partition_aocs ON sales_partition_aocs USING bitmap(status);
CREATE INDEX bmp_idx_sales_heap ON sales_heap USING bitmap(status);

-- Brin 
-- CREATE INDEX brin_idx_sales_ao ON sales_ao USING brin (sale_date) with (pages_per_range = 1); blocked by issue #558
CREATE INDEX brin_idx_sales_partition_heap ON sales_partition_heap USING brin (sale_date) with (pages_per_range = 1);
-- CREATE INDEX brin_idx_sales_aocs ON sales_aocs USING brin (sale_date) with (pages_per_range = 1); blocked by issue #558