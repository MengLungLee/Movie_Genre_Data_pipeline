# Databricks notebook source
# MAGIC %run ./config/configuration

# COMMAND ----------

# MAGIC %run ./operations/operations

# COMMAND ----------

def raw_to_bronze(rawPath: str) -> bool:
    rawDF = read_batch_raw(rawPath)
    transformRawDF = transform_raw(rawDF)
    rawToBronzeWriter = batch_writer(
        dataframe = transformRawDF, partition_column = "p_ingestdate"
    )
    rawToBronzeWriter.save(bronzePath)
    return True

# COMMAND ----------

def bronze_to_silver(spark:SparkSession, bronzePath:str, silverPath:str) -> bool:
    bronzeDF = read_batch_bronze(spark)
    transformedBronzeDF_movie = transform_bronze(bronzeDF, "movie")
    (silverCleanDF_movie, silverQuarantineDF_movie) = generate_clean_and_quarantine_dataframes(transformedBronzeDF_movie)

    silverCleanDF_genres = transform_bronze(bronzeDF, "genres")
    silverCleanDF_language = transform_bronze(bronzeDF, "language")

    bronzeToSilverWriter_movie = batch_writer(
        dataframe=silverCleanDF_movie, partition_column="p_CreatedDate", exclude_columns=["value"]
    )
    bronzeToSilverWriter_movie.save(silverPath+"movie/")

    bronzeToSilverWriter_genres = batch_writer(
        dataframe=silverCleanDF_genres, partition_column="Id"
    )
    bronzeToSilverWriter_genres.save(silverPath+"genres/")

    bronzeToSilverWriter_language = batch_writer(
        dataframe=silverCleanDF_language, partition_column="Id"
    )
    bronzeToSilverWriter_language.save(silverPath+"language/")

    update_bronze_table_status(spark, bronzePath, silverCleanDF_movie, "loaded")
    update_bronze_table_status(spark, bronzePath, silverQuarantineDF_movie, "quarantined")
    return True

# COMMAND ----------

def silver_update(spark:SparkSession, bronzePath:str, silverPath:str) -> bool:
    silverCleanDF_movie = repair_quarantined_records(
        spark, bronzeTable="movie_bronze"
    )
    bronzeToSilverWriter_movie = batch_writer(
        dataframe=silverCleanDF_movie, partition_column="p_CreatedDate", exclude_columns=["value"]
    )
    bronzeToSilverWriter_movie.save(silverPath+"movie/")
    update_bronze_table_status(spark, bronzePath, silverCleanDF_movie, "loaded")
    return True

# COMMAND ----------

raw_to_bronze(rawPath)
bronze_to_silver(spark, bronzePath, silverPath)
silver_update(spark, bronzePath, silverPath)

# COMMAND ----------

# #----------- raw->bronze
# rawDF = read_batch_raw(rawPath)
# transformRawDF = transform_raw(rawDF)
# rawToBronzeWriter = batch_writer(
#     dataframe = transformRawDF, partition_column = "p_ingestdate"
# )

# rawToBronzeWriter.save(bronzePath)

# #----------- bronze->silver
# bronzeDF = read_batch_bronze(spark)
# transformedBronzeDF_movie = transform_bronze(bronzeDF, "movie")
# (silverCleanDF_movie, silverQuarantineDF_movie) = generate_clean_and_quarantine_dataframes(transformedBronzeDF_movie)

# silverCleanDF_genres = transform_bronze(bronzeDF, "genres")
# silverCleanDF_language = transform_bronze(bronzeDF, "language")

# bronzeToSilverWriter_movie = batch_writer(
#     dataframe=silverCleanDF_movie, partition_column="p_CreatedDate", exclude_columns=["value"]
# )
# bronzeToSilverWriter_movie.save(silverPath+"movie/")

# bronzeToSilverWriter_genres = batch_writer(
#     dataframe=silverCleanDF_genres, partition_column="Id"
# )
# bronzeToSilverWriter_genres.save(silverPath+"genres/")

# bronzeToSilverWriter_language = batch_writer(
#     dataframe=silverCleanDF_language, partition_column="Id"
# )
# bronzeToSilverWriter_language.save(silverPath+"language/")

# update_bronze_table_status(spark, bronzePath, silverCleanDF_movie, "loaded")
# update_bronze_table_status(spark, bronzePath, silverQuarantineDF_movie, "quarantined")
# #------------ silver_update
# silverCleanDF_movie = repair_quarantined_records(
#     spark, bronzeTable="movie_bronze"
# )
# bronzeToSilverWriter_movie = batch_writer(
#     dataframe=silverCleanDF_movie, partition_column="p_CreatedDate", exclude_columns=["value"]
# )
# bronzeToSilverWriter_movie.save(silverPath+"movie/")
# update_bronze_table_status(spark, bronzePath, silverCleanDF_movie, "loaded")
