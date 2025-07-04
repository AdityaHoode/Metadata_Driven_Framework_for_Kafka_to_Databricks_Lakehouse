# Databricks notebook source
dbutils.widgets.text("JobName", "")
dbutils.widgets.text("JobRunID", "")

# COMMAND ----------

job_name = dbutils.widgets.get("JobName")
job_run_id = dbutils.widgets.get("JobRunID")

manual_override = False
if not manual_override:
    mappings = dbutils.widgets.get("mappings")
else:
    mappings = {
    }

# COMMAND ----------

from pyspark.sql import functions as F, types as T
import json
import re

# COMMAND ----------

# MAGIC %run "/Workspace/MDFs/Kafka to Lakehouse/functions/NB_Functions"

# COMMAND ----------

try:
    mappings = json.loads(mappings)
    start_time = spark.sql("SELECT CURRENT_TIMESTAMP()").first()[0]

    if spark.catalog.tableExists(f"{mappings['TargetCatalog']}.{mappings['TargetSchema']}.{mappings['TargetTable']}"):
        startOffset = "@latest"
    else:
        startOffset = "-1"

    startingEventPosition = {
        "offset": startOffset,  
        "seqNo": -1,            
        "enqueuedTime": None,   
        "isInclusive": True
    }

    eh_conf = {}
    event_hub_connection_string = f"Endpoint=sb://{mappings['EH_Namespace']}.servicebus.windows.net/;SharedAccessKeyName={mappings['EH_SharedAccessKeyName']};SharedAccessKey=<>"
    eh_conf["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(event_hub_connection_string)
    eh_conf["eventhubs.consumerGroup"] = mappings['EH_ConsumerGroup']
    eh_conf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

    parsed_schema = T.StructType.fromJson(json.loads(mappings['MessageBodySchema']))

    df = (
        spark.readStream
        .format("eventhubs")
        .options(**eh_conf)
        .load()
    )

    def write_to_bronze(batch_df, batchId):
        df_transformed = batch_df.withColumn("jsonData", F.from_json(F.col("body").cast("string"), parsed_schema))

        select_cols = []

        for field in parsed_schema:
            df_transformed = df_transformed.withColumn(
                field.name, F.col(f"jsonData.{field.name}"),
            )
            select_cols.append(field.name)
        
        final_df = df_transformed.select(*select_cols)

        final_df.write.format("delta").mode("append").saveAsTable(f"{mappings['TargetCatalog']}.{mappings['TargetSchema']}.{mappings['TargetTable']}")

    query = (
        df.writeStream
        .option("checkpointLocation", "abfss://mdf3@stgacct14022025.dfs.core.windows.net/streaming/mdf3_bronze_toll/checkpointLocation/")
        .foreachBatch(write_to_bronze)
        .trigger(once=True)
        .start()
    )

    end_time = spark.sql("SELECT CURRENT_TIMESTAMP()").first()[0]
    log_audit(
        job_name, job_run_id, 'Success', "", start_time, end_time
    )
except Exception as e:
    if re.search(r"UNABLE_TO_INFER_SCHEMA", str(e)):
        print("No files found after the specific cutoff time.")
    else:
        end_time = spark.sql("SELECT CURRENT_TIMESTAMP()").first()[0]
        error_details = str(e)[:1000].replace("'", "\"")
        log_audit(
            job_name, job_run_id, 'Fail', error_details, start_time, end_time
        )
        raise