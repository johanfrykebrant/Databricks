# Databricks notebook source
# A Notebook with custom made function frequently used in other notebooks

# COMMAND ----------

import pandas as pd
import random
from datetime import datetime, timedelta

def dummy_data(number_of_values = 5):
    """
    Creates and returns a spark dataframe with dummy data for testing purposes.
    There are 3 different value types - A, B and C. The input parameter number_of_values defines the number of values for each value type in the df.
    With number_of_values = 5 the df will have a total of 15 rows.
    
    The df has the following schema:
     #   Column      Non-Null Count  Dtype  
    ---  ------      --------------  -----  
     0   timestamp   15 non-null     object 
     1   value       15 non-null     float64
     2   value_type  15 non-null     object 
    """
      
    df = pd.DataFrame(columns= ['timestamp', 'value', 'value_type'])
    value_types = ['A','B','C']
    ts = datetime.now()
    for i in range(number_of_values):
        ts = ts + timedelta(seconds=1)
        for value_type in value_types:
            ts = ts + timedelta(seconds=1)
            value = random.random() * 100
            df = df.append({'timestamp' : ts,
                            'value': value,
                            'value_type': value_type},
                            ignore_index = True)
    return spark.createDataFrame(df)

# COMMAND ----------

def get_dir_content(ls_path,lvl = 1):
    """
    Recursive functions that prints entire content of a directory and its subdirectories
    """
    if lvl == 1:
        print(ls_path)
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isFile():
            print(lvl*"\t" + dir_path.name)
        elif dir_path.isDir():
            print(lvl*"\t" + dir_path.name)
            nxt_lvl = lvl +1
            get_dir_content(dir_path.path, lvl = nxt_lvl)

# COMMAND ----------

#logging

# COMMAND ----------

def createUpsertMap(keyList):
    return dict(zip(keyList, ['updates.' + key for key in keyList]))

def createKeyColumnString(keyColumnList):
    joinCondition = ''
    for col in keyColumnList:
        joinCondition += 'table.' + col + ' = updates.' + col
        if col != keyColumnList[-1]:
            joinCondition += ' AND '
    return joinCondition

def modifySchema(df, newSchemaMap):
    if newSchemaMap:
        newSchemaMap.keys()
        return df.select(
            [col(c).cast(newSchemaMap[c]) if c in newSchemaMap.keys()
             else c 
             for c in df.schema.names ])
    else:
        return df

def upsert(df, deltaTablePath, keyColumnList, partitionKey, updateList, schemaMap={}):
    newDf = modifySchema(df, schemaMap)
    if(DeltaTable.isDeltaTable(spark, deltaTablePath)):
        joinCondition = createKeyColumnString(keyColumnList)
        updateMap = createUpsertMap(updateList)
        
        #enables evolving schema for the spark session
        spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = True")
        
        deltaTable = DeltaTable.forPath(spark, deltaTablePath)
        
        deltaTable.alias("table").merge(
            newDf.alias("updates"),
            joinCondition) \
        .whenMatchedUpdate(set = updateMap) \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        if partitionKey != '':
            (newDf.write
             .format('delta')
             .partitionBy(partitionKey)
             .save(deltaTablePath))
        else:
            (newDf.write
             .format('delta')
             .save(deltaTablePath))
