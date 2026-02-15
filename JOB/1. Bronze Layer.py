from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

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


#df = spark.table("content_job.silver.silver_account_user")


sql_code1 = """
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

#spark.sql(sql_code)

#df2 = spark.table("content_job.silver.account_user_scd_type2")

#df2.display()

stg = spark.sql("SELECT * FROM df_sha256_account_user")
#stg.display()

# TO DO TOMORROW -> FINISH THE MERGE content_job.silver.account_user_scd_type2 + df_sha256_account_user (tempView)


# closing the outdated records 
# This part of code will run even with empty target table 
sql_code2 = """
MERGE INTO content_job.silver.account_user_scd_type2  AS tgt
USING (
    SELECT *, true AS is_current FROM df_sha256_account_user) src 
ON tgt.account_id = src.account_id
AND tgt.is_current = true
WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN
    UPDATE SET 
    tgt.is_current = false,
    tgt.valid_to = src.ingest_time

"""


spark.sql(sql_code2)

# data insert 
sql_code3 = """
MERGE INTO content_job.silver.account_user_scd_type2 AS tgt
USING df_sha256_account_user AS src
ON tgt.account_id = src.account_id 
AND tgt.is_current = true
WHEN NOT MATCHED THEN 
    INSERT (
        account_id,
        account_name,
        is_group,
        first_name,
        last_name,
        display_name,
        profile_url,
        profile_image_storage,
        profile_baner_storage,
        login,
        password,
        second_mail,
        sha_key,
        valid_from,
        valid_to,
        is_current
        )
    VALUES (
        src.account_id,
        src.account_name,
        src.is_group,
        src.first_name,
        src.last_name,
        src.display_name,
        src.profile_url,
        src.profile_image_storage,
        src.profile_baner_storage,
        src.login,
        src.password,
        src.second_mail,
        src.sha_key,
        src.ingest_time,
        to_timestamp('9999-12-31 23:59:59'),
        true
        )


"""


spark.sql(sql_code3)
'''

########################### ACCOUNT_DETAILS #############################
json_schema = StructType([
    StructField("userId", StringType(), True),
    StructField("friendsCount", IntegerType(), True),
    StructField("listedCount", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("rawDescription", StringType(), True),
    # Zagnieżdżona struktura accountMetadata
    StructField("accountMetadata", StructType([
        StructField("accountAge", StructType([
            StructField("createdYear", StringType(), True),
            StructField("createdMonth", StringType(), True),
            StructField("accountAgeCategory", StringType(), True)
        ])),
        StructField("verificationStatus", StructType([
            StructField("isVerified", BooleanType(), True),
            StructField("verificationConfidence", DoubleType(), True)
        ]))
    ])),
    # Zagnieżdżona struktura analyticsFlags
    StructField("analyticsFlags", StructType([
        StructField("potentialBot", BooleanType(), True),
        StructField("potentialInfluencer", BooleanType(), True)
    ])),
    # Zagnieżdżona struktura profileAnalysis
    StructField("profileAnalysis", StructType([
        StructField("profileCompletenessScore", DoubleType(), True)
    ])),
    # Zagnieżdżona struktura networkFeatures
    StructField("networkFeatures", StructType([
        StructField("networkType", StringType(), True)
    ]))
])


# Here I'm using ba
def bronze_account_details():
        return (
            spark.read
            #.format("cloudFiles")
            .format("json")
            #.option('format','delta')
            #.option("cloudFiles.format", "json")
            .schema(json_schema)
            #.option("cloudFiles.inferColumnTypes","true")
            .option("multiline","true")
            .load("/Volumes/content/landing/json_files_data/")
            .select(
                "*",
                F.current_timestamp().alias("ingest_time")
))


stg = bronze_account_details()#.filter(F.col("userId").isNotNull())
# stg.display()

w = Window.partitionBy('userId').orderBy(stg.ingest_time.desc())
stg = stg.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

#stg.display()

cols = stg.columns

#WORTH PAYING ATTENTION TO, I'M NOT POINTING USER_ID COLUMN HERE ! 
tracked_cols = [col for col in cols if col not in ['userId', 'ingest_time']]



df_sha256 = stg.withColumn("sha_key", F.sha2(F.concat_ws("|", *[F.col(col).cast(StringType()) for col in tracked_cols]), 256))

#df_sha256.display()
