################ BRONZE ####################

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



w = Window.partitionBy("account_id").orderBy(F.col("ingest_time").desc())
stg = stg.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
stg.writeTo("content_job.bronze.account_user").createOrReplace()


cols = stg.columns
tracked_cols = [col for col in cols if col not in ["account_id","ingest_time"]]#cols.remove("account_id","ingest_time")
df_sha256 = stg.withColumn("sha_key", F.sha2(F.concat_ws('|',*[F.col(col).cast(StringType()) for col in tracked_cols]),256))
df_sha256.writeTo("content_job.temp.df_sha256_account_user").createOrReplace()


########################### ACCOUNT_DETAILS #############################

json_schema = StructType([
    StructField("userId", IntegerType(), True),
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
            .load("/Volumes/content/landing/json_files_data/*.json")
            .select(
                "*",
                F.current_timestamp().alias("ingest_time")
))


stg = bronze_account_details()#.filter(F.col("userId").isNotNull())
#stg.display()


(stg.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/content/landing/checkpoints/bronze_acc_detail")
    .trigger(availableNow = True)
    .toTable("content_job.bronze.account_details")
 )
#print(q.status)
#print("\n")
#print(q.lastProgress)
#stg.display()


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

stg = (stg
        .withColumn("accountAgeCategory", F.regexp_replace("accountAgeCategory", "_", ' '))
        .withColumn("networkType", F.regexp_replace("networkType", "_", ' '))
)


cols = stg.columns 
tracked_cols = [col for col in cols if col not in ['userId', 'ingest_time']]


df_sha256 = stg.withColumn("sha_key", F.sha2(F.concat_ws("|", *[F.col(col).cast(StringType()) for col in tracked_cols]), 256))
df_sha256 = df_sha256.withColumn("location",
                                 F.when(F.col("location") == '', None).otherwise(F.col("location")))


(df_sha256.writeStream
          .format("delta")
          .outputMode("append")
          .option("checkpointLocation", "/content/landing/checkpoints/df_sha_account_details")
          .trigger(availableNow = True)
          .toTable("content_job.temp.df_sha256_account_details")
)


########################### TIME TABLE #############################


def time_table():
    return(
        spark.read
            .format("jdbc")
            .option("url", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-jdbc")) 
            .option("username", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-dblog")) 
            .option("password", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-secret")) 
            .option("dbtable", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-tab-time"))
            .load()
            .withColumn("ingest_time", F.current_timestamp())
          
        
    )
stg = time_table().filter(F.col("time_id").isNotNull())


w = Window.orderBy(stg.time_id.desc())
stg.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
stg.writeTo("content_job.bronze.time").createOrReplace()


cols = stg.columns
tracked_cols = [col for col in cols if col not in ['time_id','ingest_time']]

df_sha256 = stg.withColumn("sha_key", F.sha2(F.concat_ws('|', *[F.col(col).cast(StringType()) for col in tracked_cols]),256))
df_sha256.writeTo("content_job.temp.df_sha256_time").createOrReplace()



########################### FOLLOW RELATIONSHIP #############################


def bronze_follow_relationship():
    return(
        spark.read
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-jdbc")) 
        .option("username", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-dblog")) 
        .option("password", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-secret")) 
        .option("dbtable", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-tab-follow-relationship"))
        .load()
        .withColumn("ingest_time", F.current_timestamp())
    )


stg = bronze_follow_relationship().filter((F.col("follower_account_id").isNotNull() | (F.col("followed_account_id").isNotNull())))
stg.writeTo("content_job.bronze.follow_relationship").createOrReplace()

####### THINK ABOUT THIS PART
#w = Window.orderBy(F.col("follower_account_id").desc(), F.col("followed_account_id").desc())
#stg = stg.withColumn("rn", F.row_number().over(w))
#print(stg.select("rn").distinct().collect())

cols = stg.columns
tracked_cols = [col for col in cols if col not in ['follower_account_id', 'followed_account_id', 'ingest_time']]
df_sha256 = stg.withColumn("sha_key", F.sha2(F.concat_ws('|', *[F.col(col).cast(StringType()) for col in tracked_cols]), 256))
df_sha256.writeTo("content_job.temp.df_sha256_follow_relationship").createOrReplace()



########################### ADVERTISERS #############################


def bronze_advertisers():
    return(
        spark.read
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-jdbc")) 
        .option("username", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-dblog")) 
        .option("password", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-secret")) 
        .option("dbtable", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-tab-advertisers"))
        .load()
        .withColumn("ingest_time", F.current_timestamp())
)
    
stg = bronze_advertisers().filter(F.col("advertiser_id").isNotNull())
stg.writeTo("content_job.bronze.advertisers").createOrReplace()


cols = stg.columns
tracked_cols = [col for col in cols if col not in ['advertiser_id', 'ingest_time']]
df_sha256 = stg.withColumn("sha_key", F.sha2(F.concat_ws('|', *[F.col(col).cast(StringType()) for col in tracked_cols]), 256))
df_sha256.writeTo("content_job.temp.df_sha256_advertisers").createOrReplace()



########################### ADVERTISEMENTS #############################


def bronze_advertisements():
    return(
        spark.read
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-jdbc")) 
        .option("username", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-dblog")) 
        .option("password", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-secret")) 
        .option("dbtable", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-tab-advertisements"))
        .load()
        .withColumn("ingest_time", F.current_timestamp())
)
    
stg = bronze_advertisements().filter(F.col("advertisement_id").isNotNull())




w = Window.orderBy(stg.advertisement_id.desc())
stg.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")


stg.writeTo("content_job.bronze.advertisements").createOrReplace()

characters_original = "ąćęłńóśźżĄĆĘŁŃÓŚŹŻñáéíóúüÑÁÉÍÓÚÜ"
characters_replace = "acelnoszzACELNOSZZnaeeiouuNAEEIOUU"
stg = (stg.select("*")
          .withColumn("ad_name", F.regexp_replace(F.col("ad_name"), "_", ' '))
          .withColumn("Euro_price", F.col("price_USD").cast("float") * F.lit(0.9))
          .withColumn("pricing_model", F.when(F.col("pricing_model") == 'n/d', None).otherwise(F.col("pricing_model")))
          .withColumn("ad_name", F.translate(F.col("ad_name"), characters_original, characters_replace))
          .withColumn("ad_title", F.translate(F.col("ad_title"), characters_original, characters_replace))
          .withColumn("ad_text", F.translate(F.col("ad_text"), characters_original, characters_replace))

)

cols = stg.columns
tracked_cols = [col for col in cols if col not in ["advertisement_id","ingest_time"]]
df_sha256 = stg.withColumn("sha_key", F.sha2(F.concat_ws('|', *[F.col(col).cast(StringType()) for col in tracked_cols]), 256))

df_sha256.writeTo("content_job.temp.df_sha256_advertisements").createOrReplace()




########################### POSTS #############################


def bronze_posts():
    return(
        spark.read
             .format("jdbc")
             .option("url", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-jdbc")) 
             .option("username", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-dblog")) 
             .option("password", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-secret")) 
             .option("dbtable", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-tab-posts"))
             .load()
             .withColumn("ingest_time", F.current_timestamp())
)
    
stg = bronze_posts().filter(F.col("post_id").isNotNull())
stg.writeTo("content_job.bronze.posts").createOrReplace()

w = Window.orderBy(stg.post_id.desc())
stg.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
cols = stg.columns
tracked_cols = [col for col in cols if col not in ["post_id", "ingest_time"]]

df_sha256 = stg.withColumn("sha_key", F.sha2(F.concat_ws('|', *[F.col(col).cast(StringType()) for col in tracked_cols]), 256))
df_sha256.writeTo("content_job.temp.df_sha256_posts").createOrReplace()




########################### POST MEDIA #############################


def bronze_post_media():
    return(
        spark.read
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-jdbc")) 
        .option("username", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-dblog")) 
        .option("password", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-secret")) 
        .option("dbtable", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-tab-post-media"))
        .load()
        .withColumn("ingest_time",F.current_timestamp())
    )

stg = bronze_post_media().filter((F.col("media_id").isNotNull() | (F.col("post_id").isNotNull())))
stg.writeTo("content_job.bronze.post_media").createOrReplace()


cols = stg.columns
tracked_cols = [col for col in cols if col not in ["media_id","post_id","ingest_time"]]

df_sha256_post_media = stg.withColumn("sha_key", F.sha2(F.concat_ws('|',*[F.col(col).cast(StringType()) for col in tracked_cols]), 256))

df_sha256_post_media.writeTo("content_job.temp.df_sha256_post_media").createOrReplace()






########################### HASHTAGS #############################


def bronze_hashtags():
    return(
        spark.read
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-jdbc")) 
        .option("username", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-dblog")) 
        .option("password", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-secret")) 
        .option("dbtable", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-tab-hashtags"))
        .load()
        .withColumn("ingest_time", F.current_timestamp())
    )

stg = bronze_hashtags().filter(F.col("hashtag_id").isNotNull())

stg.writeTo("content_job.bronze.hashtags").createOrReplace()

w = Window.orderBy(stg.hashtag_id.desc())
stg.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop(F.col("rn"))



cols = stg.columns
tracked_cols = [col for col in cols if col not in ['ingest_time','hashtag_id']]

df_sha256_hashtags = stg.withColumn("sha_key", F.sha2(F.concat_ws('|',*[F.col(col).cast(StringType()) for col in tracked_cols]), 256))
df_sha256_hashtags.writeTo("content_job.temp.df_sha256_hashtags").createOrReplace()




########################### POST HASHTAG #############################

def bronze_post_hashtag():
    return(
        spark.read
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-jdbc")) 
        .option("username", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-dblog")) 
        .option("password", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-secret")) 
        .option("dbtable", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-tab-post-hashtags"))
        .load()
        .withColumn("ingest_time", F.current_timestamp())
    )

stg = bronze_post_hashtag().filter((F.col("post_id").isNotNull() | (F.col("hashtag_id").isNotNull())))
stg.writeTo("content_job.bronze.post_hashtag").createOrReplace()

### THINK ABOUT USING ROW_NUMBER HERE OR NOT

cols = stg.columns
tracked_cols = [col for col in cols if col not in ['ingest_time']]

df_sha256_post_hashtag = stg.withColumn("sha_key", F.sha2(F.concat_ws('|', *[F.col(col).cast(StringType()) for col in tracked_cols]), 256))
df_sha256_post_hashtag.writeTo("content_job.temp.df_sha256_post_hashtag").createOrReplace()

'''


########################### COMMENTS #############################

def bronze_comments():
    return(
        spark.read
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-jdbc")) 
        .option("username", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-dblog")) 
        .option("password", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-secret")) 
        .option("dbtable", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-tab-comments"))
        .load()
        .withColumn("ingest_time", F.current_timestamp())
    )

stg = bronze_comments().filter(F.col("comment_id").isNotNull())
stg.writeTo("content_job.bronze.comments").createOrReplace()

### THINK ABOUT USING ROW_NUMBER HERE OR NOT

cols = stg.columns
tracked_cols = [col for col in cols if col not in ['comment_id','ingest_time']]

df_sha256_post_hashtag = stg.withColumn("sha_key", F.sha2(F.concat_ws('|', *[F.col(col).cast(StringType()) for col in tracked_cols]), 256))
df_sha256_post_hashtag.writeTo("content_job.temp.df_sha256_comments").createOrReplace()








