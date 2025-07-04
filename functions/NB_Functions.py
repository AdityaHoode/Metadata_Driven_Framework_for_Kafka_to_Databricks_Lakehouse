# Databricks notebook source
def create_metadata_tables(catalog, schema):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.streaming_master_config (
            StreamingSource STRING,
            EH_Namespace STRING,
            EH_EventHub STRING,
            EH_SharedAccessKeyName STRING,
            EH_ConsumerGroup STRING,
            Kafka_Server STRING,
            Kafka_Topic STRING,
            MessageBodySchema STRING,
            TargetCatalog STRING,
            TargetSchema STRING,
            TargetTable STRING,
            EnableFlag BOOLEAN
        );
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.audit (
            JobName STRING,
            JobRunID STRING,
            Status STRING,
            ErrorDetails STRING,
            StartTime TIMESTAMP,
            EndTime TIMESTAMP
        );
    """)

# COMMAND ----------

def log_audit(
    job_name,
    job_run_id,
    status,
    error_details="",
    start_time=None,
    end_time=None,
    audit_table="mdf3.bronze.audit"
):
    query = f"""
    INSERT INTO
        {audit_table}
    VALUES
    (
        '{job_name}',
        '{job_run_id}',
        '{status}',
        '{error_details}',
        try_cast('{start_time}' AS TIMESTAMP),
        try_cast('{end_time}' AS TIMESTAMP)
    )
    """
    spark.sql(query)
