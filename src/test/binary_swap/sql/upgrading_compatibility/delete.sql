DELETE FROM sales_heap WHERE product_id > 1000000;

DELETE FROM sales_ao WHERE product_id > 1000000;

DELETE FROM sales_aocs WHERE product_id > 1000000;

DELETE FROM sales_partition_heap WHERE product_id > 1000000;

DELETE FROM sales_partition_ao WHERE product_id > 1000000;

DELETE FROM sales_partition_aocs WHERE product_id > 1000000;

SELECT COUNT(*) FROM sales_heap;

SELECT COUNT(*) FROM sales_ao;

SELECT COUNT(*) FROM sales_aocs;

SELECT COUNT(*) FROM sales_partition_heap;

SELECT COUNT(*) FROM sales_partition_ao;

SELECT COUNT(*) FROM sales_partition_aocs;

