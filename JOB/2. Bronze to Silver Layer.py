from pyspark.sql import functions as F
#spark.sql('USE CATALOG content_job;')
#spark.sql('USE SCHEMA bronze;')

def silver_account_user():
    return(
        spark.readStream
        #.option("cloudFiles.format","delta")
        #.option("format", "delta")
        .format("delta")
        .table("content_job.bronze.bronze_account_user")
        .select("*") # check 
        .withColumn("is_group", F.col("is_group").cast("int"))
        .withColumn("account_name", F.trim(F.col("account_name")))
        .withColumn("first_name", F.trim(F.col("first_name")))
        .withColumn("last_name", F.trim(F.col("last_name")))
        .withColumn("display_name", F.trim(F.col("display_name")))
        .withColumn("first_name", F.when(F.col("first_name") == 'n/d', None).otherwise(F.col("first_name"))) # check this 
        .withColumn("last_name", F.when(F.col("last_name") == 'n/d', None).otherwise(F.col("last_name")))
        .withColumn("display_name", F.when(F.col("display_name") == 'n/d', None).otherwise(F.col("display_name")))
        .withColumn("profile_url", F.trim(F.col("profile_url")))
        .withColumn("profile_image_storage", F.trim(F.col("profile_image_storage")))
        .withColumn("profile_baner_storage", F.trim(F.col("profile_baner_storage")))
    )

streaming_query_silver_account_user = (
    silver_account_user().writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/content_job/landing/operational_data/account_user/_checkpoint_stream")
    .toTable("content_job.silver.silver_account_user")
)

streaming_query_silver_account_user.stop()