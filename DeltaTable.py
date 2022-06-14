# Databricks notebook source
# MAGIC %md
# MAGIC Import packages and configure connection to ADLS

# COMMAND ----------

import pandas as pd
from pandas import json_normalize
import random
from datetime import datetime, timedelta
import json
import requests
from delta import *
from pyspark.sql.functions import *
import os


spark.conf.set(
	dbutils.secrets.get(scope = "sq-johan-kv", key = "saurl"),
	dbutils.secrets.get(scope = "sq-johan-kv", key = "sakey")
)

path = dbutils.secrets.get(scope = "sq-johan-kv", key = "sapath")

# COMMAND ----------

# MAGIC %run ./LibBook

# COMMAND ----------

# Check content of ADLS
get_dir_content(path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Versions and history
# MAGIC 
# MAGIC How to view delta tables version history

# COMMAND ----------

# Write some more data to the dummy_data delta file
spark_df = dummy_data(5)
spark_df.write.mode('append').format("delta").save(f"{path}/dummy_data")

# COMMAND ----------

# Load the delta table and view its content, need to be trnaformed to a spark df before .display can be used.
delta_table = DeltaTable.forPath(spark,f"{path}/dummy_data")
delta_table.toDF().display()

# COMMAND ----------

# View the history of the delta table to see what changes have been made to it and by whom
delta_table.history().display()

# COMMAND ----------

# Update every value with value_type A by adding 100 to it.
delta_table.update(
  condition = expr("value_type == 'A'"),
  set = { "value": expr("value + 100") })

# COMMAND ----------

# See changes in the history of the delta table
delta_table.history().display()

# COMMAND ----------

# See changes by viewing the data
delta_table.toDF().display()

# COMMAND ----------

# Read older vesion of delta table.
old_df = spark.read.format("delta") \
  .option("versionAsOf", 3) \
  .load(f"{path}/dummy_data")
old_df.display()

# COMMAND ----------

# Roll back data in ADLS by merging old_df into delta_table.
delta_table.alias("newData") \
  .merge(
    source = old_df.alias("oldData"),
    condition = "newData.timestamp = oldData.timestamp AND newData.value_type = oldData.value_type") \
  .whenMatchedUpdateAll() \
  .execute()

# COMMAND ----------

# See changes in the delta_table hostory
delta_table.history().display()

# COMMAND ----------

# See changes by reviewing the data
delta_table.toDF().display()

# COMMAND ----------

# Save the old data back to the ADLS
delta_table.toDF().write.mode('overwrite').format("delta").save(f"{path}/dummy_data")

# COMMAND ----------

# Load the delta table from ADLS to verivy that the overwrite was made.
delta_table = DeltaTable.forPath(spark,f"{path}/dummy_data")
#delta_table.history().display()
delta_table.toDF().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Upserting
# MAGIC Upserting table by using the merge function and udf
# MAGIC  
# MAGIC https://docs.databricks.com/delta/delta-update.html#upsert-into-a-table-using-merge&language-python

# COMMAND ----------

from pyspark.sql.functions import row_number,lit
from pyspark.sql.window import Window


# Creating some data and save it as delta to ADLS
sdf = dummy_data(5)
# create id for each row, using dummy order value
w = Window().orderBy(lit('dummy'))
sdf = sdf.withColumn("id", row_number().over(w))

sdf.write.mode('overwrite').format("delta").save(f"{path}/upsert")

# Update subset of df and append some new data to it.
sdf_new = dummy_data(2)
sdf_new = sdf_new.withColumn("id", row_number().over(w) + sdf.count())
sdf_new = sdf.union(sdf_new)
sdf_new = sdf_new.withColumn("value", 
                             when(sdf_new.value_type == "A" , sdf_new.value + 100)
                             .otherwise(sdf_new.value))


delta_table = DeltaTable.forPath(spark,f"{path}/upsert")
delta_table.toDF().display()
sdf_new.display()


# COMMAND ----------

delta_table.alias('table') \
    .merge(
        sdf_new.alias('updates'),
        'table.id = updates.id'
    ) \
    .whenMatchedUpdate(set = 
      {
          "value" : "updates.value"
      }
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

delta_table.toDF().display()
delta_table.history().display()

# COMMAND ----------

# Same thing as above can be done using custom function.
# Custom function can be found in LibBook

upsert(df = sdf_new, 
       deltaTablePath = f"{path}/upsert", 
       keyColumnList = ['id'], 
       partitionKey = '', 
       updateList = ['value'])

delta_table.history().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimize
# MAGIC https://docs.databricks.com/delta/optimizations/file-mgmt.html
# MAGIC 
# MAGIC https://docs.databricks.com/delta/optimizations/optimization-examples.html#delta-lake-on-databricks-optimizations-python-notebook
# MAGIC 
# MAGIC https://www.mssqltips.com/sqlservertip/6846/performance-tuning-apache-spark-z-ordering-data-skipping-azure-databricks/
# MAGIC 
# MAGIC The optimize function rearanges the parquet files in so that they all have a similar size (max 1 gig).
# MAGIC ZORDER partitions each parquet file based on a certain column.

# COMMAND ----------

for i in range (5):
    sdf = dummy_data(10)
    sdf.write.mode('append').format("delta").save(f"{path}/optimize")
    

# COMMAND ----------

spark.sql(f'OPTIMIZE delta.`{path}/optimize`')
# or use 
spark.sql(f'OPTIMIZE delta.`{path}/optimize` ZORDER BY (value_type)')

# COMMAND ----------

# MAGIC %md
# MAGIC #Change Data feed
# MAGIC 
# MAGIC Change feed enables querrying of of only the latset data added to the table since specified version or timestamp.
# MAGIC 
# MAGIC https://docs.databricks.com/delta/delta-change-data-feed.html

# COMMAND ----------

df = dummy_data(2)
df.write.mode('append').format("delta").save(f"{path}/dummy_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set up table and enable Change data feed
# MAGIC USE default;
# MAGIC CREATE TABLE IF NOT EXISTS dummy_table USING delta LOCATION 'abfss://databricks@sqjohansadl.dfs.core.windows.net/dummy_data';
# MAGIC ALTER TABLE dummy_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC -- See Table properties to validate ChangeDataFeed = true
# MAGIC DESCRIBE TABLE EXTENDED dummy_table;

# COMMAND ----------

delta_dummy_table = DeltaTable.forPath(spark,f"{path}/dummy_data")
delta_dummy_table.history().display()

# COMMAND ----------

# Read subset of table by defining start and end version
df = spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", 5) \
  .option("endingVersion", 5) \
  .table("dummy_table")

df.display()

# COMMAND ----------

# Read subset of table by defining start and end timestamp
df = spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingTimestamp", '2022-06-14 12:40:02.000') \
  .option("endingTimestamp", '2022-06-14 14:13:02.000') \
  .table("dummy_table")

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming
# MAGIC https://docs.databricks.com/delta/delta-streaming.html
# MAGIC 
# MAGIC https://www.youtube.com/watch?v=-OQGEc09xbY

# COMMAND ----------

# Create some dummy data and store it. This cell will be re-run several times to add new data that the stream can handle
df_newdata = dummy_data(2)
df_newdata.write.mode('append').format("delta").save(f"{path}/streamingFrom")

# COMMAND ----------

delta_table = DeltaTable.forPath(spark,f"{path}/streamingFrom")
delta_table.history().display()
#delta_table.toDF().display()

# COMMAND ----------

# initiate read stream
df = (spark
     .readStream
     .format("delta")
     .load(f"{path}/streamingFrom"))

from pyspark.sql.functions import *
# Adding some manipulation that will be done during the stream
df = df.withColumn("inserted",current_timestamp())

# COMMAND ----------

# initiate write stream
writeQuery = (df
     .writeStream
     .format("delta")
     .option("checkpointLocation",f"{path}/streamingTo/_checkpoint")
     .start(f"{path}/streamingTo"))

# Run cell to start streaming. Run above cell where data is inserted into streamFrom folder to see data being processed in real time in the dashboard.
# Cluster needs to stay on for stream to continue.

# COMMAND ----------

# Visualize the data that was processed by the stream
delta_table = DeltaTable.forPath(spark,f"{path}/streamingTo")
delta_table.history().display()
#delta_table.toDF().display()

# COMMAND ----------

# Instead of requiring the cluster to stay on, a trigger can be set so that the stream only runs one batch and then shuts down.
# The next time the cell is run, only new rows that has been added will be handled by the stream.
# Add some new data to the StreamingFrom table before this cell is run 
writeQuery = (df
     .writeStream
     .format("delta")
     .option("checkpointLocation",f"{path}/streamingTo/_checkpoint")
     .trigger(once=True)
     .start(f"{path}/streamingTo"))
