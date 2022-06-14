-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Creating unmanaged & managed table
-- MAGIC 
-- MAGIC https://docs.databricks.com/spark/2.x/spark-sql/language-manual/create-table.html
-- MAGIC 
-- MAGIC https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html#parameters
-- MAGIC 
-- MAGIC https://docs.databricks.com/sql/language-manual/sql-ref-datatypes.html

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC 	dbutils.secrets.get(scope = "sq-johan-kv", key = "saurl"),
-- MAGIC 	dbutils.secrets.get(scope = "sq-johan-kv", key = "sakey")
-- MAGIC )
-- MAGIC 
-- MAGIC path = dbutils.secrets.get(scope = "sq-johan-kv", key = "sapath")

-- COMMAND ----------

-- Creating a schema to put my tables in. This is not necessary, might aswell use the default schema
CREATE SCHEMA IF NOT EXISTS ts
COMMENT "test schema";

SHOW SCHEMAS;
DESCRIBE SCHEMA EXTENDED ;

-- COMMAND ----------

USE ts;
-- Create a managed table
-- A managed table will be stored in the storage account associated with the deltabricks instance. -> DBFS/user/hive/warehouse/ts.db
-- When creating a managed table, schema and other properties can be specified, similar to when creating a table in SQL server.
DROP TABLE IF EXISTS managed_table;
CREATE TABLE IF NOT EXISTS managed_table
  (value_id LONG GENERATED ALWAYS AS IDENTITY COMMENT 'Auto increment id' 
  ,timestamp TIMESTAMP
  ,value DOUBLE
  ,value_type STRING)
  PARTITIONED BY (value_type)
  TBLPROPERTIES ('Some key' = 'Some value')
  COMMENT 'table used for testing';

DESCRIBE TABLE EXTENDED managed_table;

-- COMMAND ----------

-- Create an external (unmanaged) table
-- An unmanaged table will be stored in the path specified in the creation.
-- An unmanged table is entirely dependent on the properties of the data stored in the location specified. No properties or schema can be defined when creating the table.
DROP TABLE IF EXISTS external_table;

CREATE TABLE IF NOT EXISTS external_table USING delta LOCATION 'abfss://databricks@sqjohansadl.dfs.core.windows.net/upsert';

DESCRIBE TABLE EXTENDED managed_table;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

USE ts;
DROP TABLE dummy_table
DROP TABLE dummt_table;
DROP TABLE external_table;
DROP SCHEMA ts;
