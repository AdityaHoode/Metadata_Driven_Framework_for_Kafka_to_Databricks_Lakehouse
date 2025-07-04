# Databricks notebook source
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

import json
import datetime

# COMMAND ----------

file_master_config = spark.sql(f"SELECT * FROM {catalog}.{schema}.streaming_master_config WHERE EnableFlag = 1").collect()
file_master_config_dict = [row.asDict() for row in file_master_config]
for item in file_master_config_dict:
    for key, value in item.items():
        if isinstance(value, datetime.datetime):
            item[key] = value.isoformat()
dbutils.jobs.taskValues.set(key="mappings", value=file_master_config_dict)