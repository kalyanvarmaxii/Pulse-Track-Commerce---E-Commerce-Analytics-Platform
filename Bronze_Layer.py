# Databricks notebook source
# MAGIC %md
# MAGIC ### **INCREMENTAL DATA INGESTION**

# COMMAND ----------

dbutils.widgets.text("src","")

# COMMAND ----------

src_values = dbutils.widgets.get("src")
src_values

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.schemaLocation",f"/Volumes/workspace/bronze/bronzevolume/{src_values}/checkpoint") \
  .option("cloudFiles.schemaEvolutionMode", "rescue") \
  .load(f"/Volumes/workspace/raw/rawdata/{src_values}/")

# COMMAND ----------

df.writeStream \
  .format("delta") \
  .trigger(once=True) \
  .outputMode("append") \
  .option("checkpointLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_values}/checkpoint/") \
  .option("path", f"/Volumes/workspace/bronze/bronzevolume/{src_values}/data").start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta .`/Volumes/workspace/bronze/bronzevolume/BOOKINGS/data`