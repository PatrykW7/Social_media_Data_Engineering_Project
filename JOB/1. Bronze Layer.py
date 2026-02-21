from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType


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

#stg.display()

#stg = spark.table("content_job.bronze.bronze_account_user")
#stg.display()
cols = stg.columns
#a = a.remove("account_id")
tracked_cols = [col for col in cols if col not in ["account_id","ingest_time"]]#cols.remove("account_id","ingest_time")
#print(tracked_cols)

df_sha256 = stg.withColumn("sha_key", F.sha2(F.concat(F.col("account_id").cast(StringType()), F.col("account_name"), F.col("is_group").cast(StringType())),256))
     #.withColumn("sha_key", F.concat(*[F.col(col).cast(StringType()) for col in tracked_cols]))
     # NEED TO SPECIFY ALL TRACKED COLUMNS 
#df_sha256.display()

df_sha256.writeTo("content_job.temp.df_sha256_account_user").createOrReplace()


#df = spark.table("content_job.silver.silver_account_user")


#spark.sql(sql_code)

#df2 = spark.table("content_job.silver.account_user_scd_type2")

#df2.display()

#stg = spark.sql("SELECT * FROM df_sha256_account_user")
#stg.display()

# TO DO TOMORROW -> FINISH THE MERGE content_job.silver.account_user_scd_type2 + df_sha256_account_user (tempView)



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
            spark.readStream
            .format("cloudFiles")
            #.format("json")
            #.option('format','delta')
            .option("cloudFiles.format", "json")
            .schema(json_schema)
            #.option("cloudFiles.inferColumnTypes","true")
            .option("multiline","true")
            .load("/Volumes/content/landing/json_files_data/test_dane1.json")
            .select(
                "*",
                F.current_timestamp().alias("ingest_time")
))


stg = bronze_account_details()#.filter(F.col("userId").isNotNull())
# stg.display()


(stg.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/content/landing/checkpoints/bronze_account_details")
    .trigger(availableNow = True)
    .toTable("content_job.bronze.bronze_account_details")
 )

# stg.display()
#stg.writeTo("content_job.bronze.bronze_account_details").createOrReplace()



#stg = spark.read.table("content_job.bronze.bronze_account_details")

stg = stg.select(
                "userId",
                F.date_format(F.make_date(F.col("accountMetadata.accountAge.createdYear").cast("int"), F.col("accountMetadata.accountAge.createdMonth").cast("int"), F.lit(1)),'yyyy-MM').alias("account_creation_year_month"),
                "accountMetadata.accountAge.accountAgeCategory",
                "accountMetadata.verificationStatus.isVerified",
                "accountMetadata.verificationStatus.verificationConfidence",
                "analyticsFlags.potentialBot",
                "analyticsFlags.potentialInfluencer",
                "friendsCount",
                "listedCount",
                "location",
                "rawDescription",
                "profileAnalysis.profileCompletenessScore",
                "networkFeatures.networkType",
                "ingest_time"
            )
            #.withColumn("accountAgeCategory", F.regexp_replace("accountAgeCategory", "_", ' ')) \
            #.withColumn("networkType", F.regexp_replace("networkType", "_", ' '))


stg = (stg
        .withColumn("accountAgeCategory", F.regexp_replace("accountAgeCategory", "_", ' '))
        .withColumn("networkType", F.regexp_replace("networkType", "_", ' '))
)


''' # FIGURE TO COMMENT THIS OR NOT
w = Window.partitionBy('userId').orderBy(stg.ingest_time.desc())
stg = stg.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
'''
#stg.display()

cols = stg.columns

#WORTH PAYING ATTENTION TO, I'M NOT POINTING USER_ID COLUMN HERE ! 
tracked_cols = [col for col in cols if col not in ['userId', 'ingest_time']]



df_sha256 = stg.withColumn("sha_key", F.sha2(F.concat_ws("|", *[F.col(col).cast(StringType()) for col in tracked_cols]), 256))


#print(f"dtypes: {df_sha256.dtypes}")

df_sha256 = df_sha256.withColumn("location",
                                 F.when(F.col("location") == '', None).otherwise(F.col("location")))


# df_sha256.display()


#df_sha256.writeTo("content_job.temp.df_sha256_account_details").createOrReplace()

(df_sha256.writeStream
          .format("delta")
          .outputMode("append")
          .option("checkpointLocation", "/content/landing/checkpoints/df_sha256_account_details")
          .trigger(availableNow = True)
          .toTable("content_job.temp.df_sha256_account_details")
)

















