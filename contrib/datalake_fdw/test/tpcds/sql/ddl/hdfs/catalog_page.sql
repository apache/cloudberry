DROP TABLE IF EXISTS catalog_page_w;
DROP EXTERNAL TABLE IF EXISTS er_catalog_page_hdfs;

CREATE TABLE catalog_page_w(like catalog_page);
CREATE FOREIGN TABLE er_catalog_page_hdfs
(
    cp_catalog_page_sk        integer                       ,
    cp_catalog_page_id        char(16)                      ,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/catalog_page/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');