# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Define Data Path

# COMMAND ----------

# Set up the path
pipelinePath = f"/dbpipeline/data/"

rawPath = f"/mnt/movie/files_from_Mao/"
bronzePath = pipelinePath + "bronze/"
silverPath = pipelinePath + "silver/"
silverQuarantinePath = pipelinePath + "silverQuarantine/"

# COMMAND ----------

# MAGIC %md
# MAGIC Configuration Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS Movie")
spark.sql(f"USE Movie")

# COMMAND ----------



# COMMAND ----------


