# Databricks notebook source
# MAGIC %md
# MAGIC # Seting up
# MAGIC Importing packages

# COMMAND ----------

import pandas as pd
from pandas import json_normalize
import random
from datetime import datetime, timedelta
import json
import requests
from delta import *
%run ./LibBook

# COMMAND ----------

# Configuring session to acquire access to storage.
# Here it is done using storage account key. Can be done in different, more secure ways -> https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-use-databricks-spark
# Key and strings are imported from Azure key vault
spark.conf.set(
	dbutils.secrets.get(scope = "sq-johan-kv", key = "saurl"),
	dbutils.secrets.get(scope = "sq-johan-kv", key = "sakey")
)

path = dbutils.secrets.get(scope = "sq-johan-kv", key = "sapath")

# Check content of dir
dbutils.fs.ls(f"{path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Create dataframe and fill it with dummy data

# COMMAND ----------

#Creating pandas dataframe
pandas_df = dummy_data(20)
        
#Converting pandas dataframe to spark dataframe
spark_df = spark.createDataFrame(pandas_df)
spark_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Write data
# MAGIC to ADLS ..

# COMMAND ----------

# MAGIC %md
# MAGIC Connect to ADLS

# COMMAND ----------

# Configuring session to acquire access to storage.
# Here it is done using storage account key. Can be done in different, more secure ways -> https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-use-databricks-spark
# Key and strings are imported from Azure key vault
spark.conf.set(
	dbutils.secrets.get(scope = "sq-johan-kv", key = "saurl"),
	dbutils.secrets.get(scope = "sq-johan-kv", key = "sakey")
)

path = dbutils.secrets.get(scope = "sq-johan-kv", key = "sapath")

# Check content of dir
dbutils.fs.ls(f"{path}")

# COMMAND ----------

# Write to ADLS as parquet file
spark_df.write.mode('append').parquet(f"{path}/dummy_data")

# COMMAND ----------

# MAGIC %md
# MAGIC Partitioning 

# COMMAND ----------

path = dbutils.secrets.get(scope = "sq-johan-kv", key = "sapath")

df = dummy_data(number_of_values = 10)
spark_df = spark.createDataFrame(df)

# Single partition
spark_df.write.mode('append').partitionBy("timestamp").format("parquet").save(f"{path}/dummy_dataByTimestamp")
# Multiple partitions
spark_df.write.mode('append').partitionBy(["value_type","timestamp"]).format("parquet").save(f"{path}/dummy_dataByMultiple")

# COMMAND ----------

# Write to ADLS as delta file
# if no format is defined, it is delta by default. Best pracise to still define it as delta for clarity. 
spark_df.write.mode('append').format("delta").save(f"{path}/dummy_data")

# COMMAND ----------

# MAGIC %md
# MAGIC to SQL DB

# COMMAND ----------

url = dbutils.secrets.get(scope = "sq-johan-kv", key = "dburl")
user = dbutils.secrets.get(scope = "sq-johan-kv", key = "dbusername")
pw = dbutils.secrets.get(scope = "sq-johan-kv", key = "dbpw")
                          
spark_df.write \
    .format("jdbc") \
    .mode("append") \
    .option("url", url) \
    .option("dbtable", "dbo.dummy_data") \
    .option("user", user) \
    .option("password", pw) \
    .save()


# COMMAND ----------

# MAGIC %md
# MAGIC # Read data
# MAGIC from ADLS

# COMMAND ----------

# Loading the entire parquet file into a dataframe
df = spark.read.parquet(f"{path}/dummy_data.parquet")
df.display()

# COMMAND ----------

# Query the parquet file directly defore loading it into databricks.
str = "parquet.`" + dbutils.secrets.get(scope = "sq-johan-kv", key = "sapath") + "/dummy_dataByTimestamp.parquet" + "`"
df = spark.sql("select * from {} where value_type = 'A'".format(str))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC from SQL DB

# COMMAND ----------

# Query sqlDB and load into df
df = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("user", user) \
    .option("password", pw) \
    .option("query", "select * from dbo.dummy_data where value_type = 'A'") \
    .load()

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Read json from API and format to df

# COMMAND ----------

# Using SMHIs open API to get wather forcast för Malmö
longitude = '13.07'
latitude = '55.6'
SMHI_FORECAST = "https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/" + longitude + "/lat/" + latitude + "/data.json"

r = requests.get(SMHI_FORECAST)
jobj = r.json()

jobj

# COMMAND ----------

df = pd.json_normalize(jobj["timeSeries"],
                      record_path = 'parameters',
                      meta='validTime')

df["parameter_type"] = df['name'] +  " (" + df['unit'] + ")"
df['new_values'] = df.apply(lambda row : row['values'][0],  axis = 1)

df = df.drop(columns=['levelType','level','unit','name','values'])

df.pivot(index='validTime', columns='parameter_type', values='new_values')

df.head()
