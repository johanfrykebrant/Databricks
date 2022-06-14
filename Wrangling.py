# Databricks notebook source
# MAGIC %md
# MAGIC Some common data wrangling procedures

# COMMAND ----------

#upsert, createdTimestamp modifiedTimestamp

# COMMAND ----------

# delta upp timestamp i date, month, year, time osv för bättre partitionerings möjligheter.

# COMMAND ----------

# Window functions 
https://sparkbyexamples.com/pyspark/pyspark-window-functions/

# COMMAND ----------

# df.withColumn('mycol', x+2)

df.select(1,2,34,5, F.add(x+2).alias('MyCol'), 2,6,78,95)

# COMMAND ----------

# statisk kalendertabell & relativ kalendertabell
