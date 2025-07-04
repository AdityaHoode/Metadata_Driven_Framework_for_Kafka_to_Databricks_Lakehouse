# Databricks notebook source
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %run "/Workspace/MDFs/Kafka to Lakehouse/functions/NB_Functions"

# COMMAND ----------

create_metadata_tables(catalog, schema)