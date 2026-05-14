# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.rawdata

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/workspace/raw/rawdata/FLIGHTS')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME workspace.bronze.bronzevolume;
# MAGIC     
# MAGIC CREATE VOLUME workspace.silver.silvervolume;
# MAGIC
# MAGIC CREATE VOLUME workspace.gold.goldvolume;