# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_unixtime,
    lit,
    when,
    explode,
    abs
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

def read_batch_raw(rawPath: str) -> DataFrame:
    df = spark.read.format("json").option("multiline", "true").load(rawPath)
    return df.select(explode(df.movie))

# COMMAND ----------

def transform_raw(rawDF: DataFrame) -> DataFrame:
    return rawDF.select(
        col("col").alias("value"),
        lit("movie.json").alias("datasource"),
        current_timestamp().alias("ingesttime"),
        lit("new").alias("status"),
        current_timestamp().cast("date").alias("p_ingestdate")    
    )

# COMMAND ----------

def batch_writer(dataframe: DataFrame, partition_column: str, exclude_columns: List =[], mode: str = "append") -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )
        .write.format("delta")
        .mode(mode)
        .partitionBy(partition_column)
    )

# COMMAND ----------

def read_batch_bronze(spark: SparkSession) -> DataFrame:
    return spark.read.table("movie_bronze").filter(col("status") == "new")

# COMMAND ----------

def fix_genres(dataframe: DataFrame) -> DataFrame:
    return (dataframe.select(col("col.*")).distinct()).select(col("id").alias("Id"), col("name")).filter(col("name") != "").orderBy("Id")


# COMMAND ----------

def transform_bronze(bronze: DataFrame, table_name: str ) -> DataFrame:
    bronzeAgumentedDF = bronze.select("value", "value.*")
    
    if table_name == 'movie':
        return bronzeAgumentedDF.select(
        col("value"),
        col("Id").cast("INTEGER"),
        col("Budget"),
        col("Revenue"),
        col("RunTime").cast("INTEGER"),
        col("Price"),
        col("Title"),
        col("Overview"),
        col("Tagline"),
        col("ImdbUrl"),
        col("TmdbUrl"),
        col("PosterUrl"),
        col("BackdropUrl"),
        col("ReleaseDate").cast("DATE"),
        col("CreatedDate").cast("DATE").alias("p_CreatedDate"),
        col("UpdatedDate"),
        col("UpdatedBy"),
        col("CreatedBy"),
        col("Genres.id").alias("Genres_Id"),
        lit(1).alias("Language_Id")   
        ).withColumn("Budget", when((bronzeAgumentedDF.Budget < 100000), 100000).otherwise(bronzeAgumentedDF.Budget))
    
    elif table_name == 'genres':
        genres_explode = bronzeAgumentedDF.select(
            explode(col("genres"))
        )
        return fix_genres(genres_explode)
    elif table_name == 'language':
        return bronzeAgumentedDF.select(
            col("OriginalLanguage")
        ).distinct().select(
            lit(1).alias("Id"),
            col("OriginalLanguage")
        )
    else:
        return False

# COMMAND ----------

def generate_clean_and_quarantine_dataframes(dataframe: DataFrame) -> (DataFrame, DataFrame):
    return (
        dataframe.distinct().filter(col("RunTime") >= 0),
        dataframe.distinct().filter(col("RunTime") < 0),
    )


# COMMAND ----------

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:

    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    dataframeAugmented = dataframe.withColumn("status", lit(status))

    update_match = "bronze.value = dataframe.value"
    update = {"status": "dataframe.status"}

    (
        bronzeTable.alias("bronze")
        .merge(dataframeAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True


# COMMAND ----------

def repair_quarantined_records(spark: SparkSession, bronzeTable: str) -> DataFrame:
    
    bronzeQuarantinedDF = spark.read.table(bronzeTable).filter("status = 'quarantined'")
    bronzeQuarTransDF = bronzeQuarantinedDF.select(
                        col("value"),
                        col("value.*")
                    )
    repairDF = bronzeQuarTransDF.distinct().withColumn("RunTime", abs(col("RunTime")))
    
    silverCleanedDF = repairDF.select(
        col("value"),
        col("Id").cast("INTEGER"),
        col("Budget"),
        col("Revenue"),
        col("RunTime").cast("INTEGER"),
        col("Price"),
        col("Title"),
        col("Overview"),
        col("Tagline"),
        col("ImdbUrl"),
        col("TmdbUrl"),
        col("PosterUrl"),
        col("BackdropUrl"),
        col("ReleaseDate").cast("DATE"),
        col("CreatedDate").cast("DATE").alias("p_CreatedDate"),
        col("UpdatedDate"),
        col("UpdatedBy"),
        col("CreatedBy"),
        col("Genres.id").alias("Genres_Id"),
        lit(1).alias("Language_Id")
    )
    return silverCleanedDF


# COMMAND ----------



# COMMAND ----------


