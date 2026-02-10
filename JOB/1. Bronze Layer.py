from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType
'''
def bronze_account_user():
    return (
        spark.read
        .format("jdbc") 
        .option("url", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-jdbc")) 
        .option("username", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-dblog")) 
        .option("password", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-secret")) 
        .option("dbtable", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-tab-acc-users")) 
        .load()
        .withColumn("ingest_time", F.current_timestamp())
    )

stg = bronze_account_user().filter(F.col("account_id").isNotNull())

#if True:

#    print("Udalo sie")
#else:
#    print("PASS")

w = Window.partitionBy("account_id").orderBy(F.col("ingest_time").desc())

stg = stg.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
stg.writeTo("content_job.bronze.bronze_account_user").createOrReplace()

stg.display()

stg = spark.table("content_job.bronze.bronze_account_user")
#stg.display()
cols = stg.columns
#a = a.remove("account_id")
tracked_cols = [col for col in cols if col not in ["account_id","ingest_time"]]#cols.remove("account_id","ingest_time")
print(tracked_cols)

df_sha256 = stg.withColumn("sha_key", F.sha2(F.concat(F.col("account_id").cast(StringType()), F.col("account_name"), F.col("is_group").cast(StringType())),256))
     #.withColumn("sha_key", F.concat(*[F.col(col).cast(StringType()) for col in tracked_cols]))
     # NEED TO SPECIFY ALL TRACKED COLUMNS 
df_sha256.display()

df_sha256.createOrReplaceTempView("df_sha256_account_user")


df = spark.table("content_job.silver.silver_account_user")


sql_code = """
CREATE TABLE IF NOT EXISTS content_job.silver.account_user_scd_type2 (
       account_id INT,
       account_name STRING,
       is_group BOOLEAN,
       first_name STRING,
       last_name STRING,
       display_name STRING,
       profile_url STRING,
       profile_image_storage STRING,
       profile_baner_storage STRING,
       login STRING,
       password STRING,
       second_mail STRING,
       sha_key STRING,
       valid_from TIMESTAMP,
       valid_to TIMESTAMP,
       is_current BOOLEAN
    )
USING DELTA;
"""

spark.sql(sql_code)
'''
#df2 = spark.table("content_job.silver.account_user_scd_type2")

#df2.display()

res = spark.sql("SELECT * FROM df_sha256_account_user")
res.display()

# TO DO TOMORROW -> FINISH THE MERGE content_job.silver.account_user_scd_type2 + df_sha256_account_user (tempView)
#sql_code = """
#MERGE INTO content_job.silver.account_user_scd_type2
#USING df_sha256_account_user
#ON 

#"""
