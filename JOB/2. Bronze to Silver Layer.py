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

    )

silver_account_user().display()