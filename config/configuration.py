# Databricks notebook source
# MAGIC %md ### Setting up Credential file 
# MAGIC 
# MAGIC Get key from credential file to access the data in Azure Data Lake Storage Gen 2

# COMMAND ----------

# MAGIC %run ./credential

# COMMAND ----------

PipelinePath = "abfss://movie@westusdemo.dfs.core.windows.net/"

# COMMAND ----------

rawPath = PipelinePath + "RawData/"
bronzePath = PipelinePath + "Bronze/"
silverPath = PipelinePath + "Silver/"

checkpointPath = PipelinePath + "checkpoints/"
bronzeCheckpoint = checkpointPath + "Bronze/"
silverCheckpoint = checkpointPath + "Silver/"
