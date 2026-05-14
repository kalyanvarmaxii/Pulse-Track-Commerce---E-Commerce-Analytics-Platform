# Databricks notebook source
src_array = [
    
    {"src": "BOOKINGS"},
    {"src": "FLIGHTS"},
    {"src": "CUSTOMERS"},
    {"src": "AIRPORTS"},


 ]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "output_key", value =src_array)