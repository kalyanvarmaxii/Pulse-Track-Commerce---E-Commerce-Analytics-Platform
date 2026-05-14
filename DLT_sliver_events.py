# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

@dlt.table(
    name = "stage_events"

)
def stage_events():
    df=spark.readStream.format('delta')\
        .load('/Volumes/pulsetrack/bronze/bronzevolume/events/data/')
    return df
    

# COMMAND ----------

@dlt.view(
    name = "trans_events"
)
def trans_events():
    df = dlt.read_stream("stage_events")
    df = df.withColumn('user_id', col('user_id').cast('int'))\
           .withColumn('event_id', col('event_id').cast('int'))\
           .withColumn('product_id', col('product_id').cast('int'))\
           .withColumn('event_time',col('event_time').cast('timestamp'))\
           .withColumn('session_id', col('session_id').cast('int'))\
           .drop('_rescued_data')


    return df

# COMMAND ----------

rules = {

    "valid_event_id": "event_id IS NOT NULL",

    "valid_user_id": "user_id IS NOT NULL",

    "valid_event_type":
        "event_type IN ('view', 'click', 'add_to_cart', 'purchase')",

    "valid_event_time":
        "event_time IS NOT NULL",

    "valid_session_id":
        "session_id IS NOT NULL"
}


# COMMAND ----------

@dlt.tables(
    name = "sliver_layer"
)
@dlt.except_all.drop('rules')
def sliver_layer():
    df = dlt.read_stream("trans_events")
    return df