from pyspark.sql import functions as F
import dlt


#spark.sql("USE CATALOG content")
##### account_user #####
@dlt.table(
    name = "silver_account_user_clean",
    table_properties = {"schema" : "silver"},
    comment = "silver table for account_user",
    temporary = True
)

@dlt.expect("is_group","is_group IS NOT NULL")
@dlt.expect_or_drop("account_id","account_id IS NOT NULL")
@dlt.expect_or_fail("account_id>0", "account_id >= 0 ")
@dlt.expect_or_fail("login","login IS NOT NULL")
@dlt.expect_or_fail("password","password IS NOT NULL")
def silver_account_user_clean():
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table("LIVE.bronze_account_user")  
        #.filter(F.col("_change_type") == "insert")
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

dlt.create_streaming_table(
    name = "silver_account_user",
    comment = "SCD Type 2 account_user",
    table_properties = {"schema" : "silver"}
)

dlt.apply_changes(
    target = "silver_account_user",
    source = "silver_account_user_clean",
    keys = ["account_id"],
    sequence_by = "ingest_time",#,#"account_id",
    stored_as_scd_type = "2",
    track_history_column_list = ["account_name","is_group","first_name","last_name","display_name"],
    except_column_list = ["ingest_time"]#["ingest_time","_commit_version","_commit_timestamp"]
)




'''
### follow_relationship

@dlt.table(
    name = "silver_follow_clean",
    table_properties = {"schema" : "silver"},
    comment = "silver table for follow_relationship"
    
)

@dlt.expect("self_following","follower_account_id <> followed_account_id")
@dlt.expect("date_null", "followed_at_time_id IS NOT NULL")
def silver_follow_clean():
    return(
        spark.read.table("LIVE.bronze_follow_relationship")
        .select("*")
    )


dlt.create_streaming_table(
    name = "silver_follow",
    comment = "SCD Type 2 follow_relationship",
    table_properties = {"schema" : "silver"}
)


dlt.apply_changes(
    target = "silver_follow",
    source = "silver_follow_clean",
    keys = ["follower_account_id", "followed_account_id"],
    sequence_by = "followed_at_time_id", # -> check,
    stored_as_scd_type = "2"
)



##### ADVERTISERS #####

# transformacje - destination_group usuwamy _ 
# transformacje - billing_account_code zostawiamy same cyfry


@dlt.table(
    name = "silver_advertisers_clean",
    comment = "silver table for advertisers",
    table_properties = {"schema" : "silver"}
)

@dlt.expect("advertiser_id", "advertiser_id IS NOT NULL")
@dlt.expect("billing_account_code", "billing_account_code IS NOT NULL")
@dlt.expect("billing_status", "billing_status IS NOT NULL")
def silver_advertisers():
    return(
        spark.readStream.table("LIVE.bronze_advertisers")
        .select("*")
        .withColumn("destination_group", F.regexp_replace(F.col("destination_group"),"_",' '))
        .withColumn("billing_account_code", F.regexp_replace(Fo.col("billing_account_code"), "[^0-9]", ' '))
    )


dlt.create_streaming_table(
    name = "silver_advertisers",
    comment = "SCD Type 2 advertisers",
    table_properties = {"schema" : "silver"}
)


dlt.apply_changes(
    target = "silver_advertisers",
    source = "silver_advertisers_clean",
    keys = "advertiser_id",
    sequence_by = "advertiser_id" # check 
)




@dlt.table(
    name = "silver_advertisements_clean",
    comment = "silver table for advertisements",
    table_properties = {"schema" : "silver"}
)

@dlt.expect("adveriser_id", "adveriser_id IS NOT NULL")
@dlt.expect("advertisement_id","adverisement_id IS NOT NULL")
@dlt.expect("price", "price > 0")
@dlt.expect("advertisement_id", "advertisement_id > 0")
@dlt.expect("advertisement_id", "adverisement_id > 0")
@dlt.expect("end_at", "end_at IS NOT NULL")
def silver_advertisements():
    characters_original = "ąćęłńóśźżĄĆĘŁŃÓŚŹŻñáéíóúüÑÁÉÍÓÚÜ"
    characters_replace = "acelnoszzACELNOSZZnaeeiouuNAEEIOUU"
    return(
        spark.readStream.table("LIVE.bronze_advertisements")
        .select("*")
        .withColumn("ad_name", F.regexp_replace(F.col("ad_name"), "_", ' '))
        .withColumn("Euro_price", F.col("price") * 0.9)
        .withColumn("pricing_model", F.when(F.col("pricing_model") == 'n/d', None).otherwise(F.col("pricing_model")))
        .withColumn("ad_name", F.translate(F.col("ad_name"), characters_original, characters_replace))
        .withColumn("ad_title", F.translate(F.col("ad_title"), characters_original, characters_replace))
        .withColumn("ad_text", F.translate(F.col("ad_text"), characters_original, characters_replace))
    )

dlt.create_streaming_table(
    name = "silver_adverisements",
    comment = "SCD Type 2 advertisements",
    table_properties = {"schema" : "silver"}
)


dlt.apply_changes(
    target = "silver_advertisements",
    source=  "silver_advertisements_clean",
    keys = "advertisement_id",
    sequence_by = "created_at_time",
    stored_as_scd_type = "2"
)



@dlt.table(
    name = "silver_posts_clean",
    comment = "silver table for posts",
    table_properties = {"schema" : "silver"}
)

@dlt.expect("post_id", "post_id IS NOT NULL")
@dlt.expect("post_id_>0", "post_id > 0")
@dlt.expect_or_fail("author_id", "author_id IS NOT NULL")
@dlt.expect("created_at_time", "created_at_time IS NOT NULL")
def silver_posts():
    return(
        spark.readStream.table("LIVE.bronze_posts")
        .select("*")
        .withColumn("post_text", F.trim(F.col("post_text")))
        .withColumn("visibility", F.when(F.col("visibility") =='n/d', None).otherwise(F.col("visibility")))
    )


dlt.create_streaming_table(
    name = "silver_posts",
    comment = "SCD Type 2 posts",
    table_properties = {"schema" : "silver"}
)

dlt.apply_changes(
    target = "silver_posts",
    source = "silver_posts_clean",
    keys = "post_id",
    sequence_by = "created_at_time",
    stored_as_scd_type = "2"
)




@dlt.table(
    name = "silver_post_media",
    comment = "silver_post_media_clean",
    table_properties = {"schema" : "silver"}
)

@dlt.expect("media_url", "media_url IS NOT NULL")
@dlt.expect("post_id", "post_id IS NOT NULL")
def silver_post_media():
    return(
        spark.readStream.table("LIVE.bronze_post_media")
        .select("*")
    )

dlt.create_streaming_table(
    name = "silver_post_media",
    comment = "SCD Type 2 post_media",
    table_properties = {"schema" : "silver"}
)


dlt.apply_changes(
    target = "silver_post_media",
    source = "silver_post_media_clean",
    keys = ["media_id", "post_id"], ### check
    sequence_by = ["media_id", "post_id"], ### check,
    stored_as_scd_type = "2"
)



@dlt.table(
    name = "silver_hashtags_clean",
    comment = "silver table for hashtags",
    table_properties = {"schema" : "silver"}
)

@dlt.expect("hashtag_id", "hashtag_id IS NOT NULL")
@dlt.expect("hashtag_id>0", "hashtag_id > 0")
@dlt.expect("first_use_time", "first_use_time IS NOT NULL")
def silver_hashtags():
    return(
        spark.readStream.table("LIVE.bronze_hashtags")
        .select("*")
        .withColumn("tag_text", F.trim(F.col("tag_text")))
    )


dlt.create_streaming_table(
    name = "silver_hashtags",
    comment = "SCD Type 2 for hashtags",
    table_properties = {"schema" : "silver"}
)

dlt.apply_changes(
    target = "silver_hashtags",
    source = "silver_hashtags_clean",
    keys = "hashtag_id",
    sequence_by= "first_time_use",
    stored_as_scd_type = "2"
)





@dlt.table(
    name = "silver_post_hashtags_clean",
    comment = "silver table for post_hashtags",
    table_properties = {"schema" : "silver"}
)


@dlt.expect("post_id", "post_id IS NOT NULL")
@dlt.expect("post_id>0", "post_id > 0 ")
@dlt.expect("hashtag_id", "hashtag_id IS NOT NULL")
@dlt.expect("hashtag_id>0", "hashtag_id > 0")

def silver_post_hashtags():
    return(
        spark.readStream.table("LIVE.bronze_post_hashtags")
        .select("*")
    )


dlt.create_streaming_table(
    name = "silver_post_hashtags",
    comment = "SCD Type 2 for post_hashtags",
    table_properties = {"schema" : "silver"}
)


dlt.apply_changes(
    target = "silver_post_hashtags",
    source = "silver_post_hashtags_clean",
    keys = ["post_id", "hashtag_id"],
    sequence_by = ["post_id", "hashtag_id"],
    stored_as_scd_type = "2"
)




@dlt.table(
    name = "silver_comments_clean",
    comment = "silver table for comments",
    table_properties = {"schema" : "silver"}
)

@dlt.expect("comment_id", "comment_id IS NOT NULL")
@dlt.expect("comment_id>0", "comment_id > 0")
@dlt.expect("author_account_id", "author_account_id IS NOT NULL")
@dlt.expect("author_account_id>0", "author_account_id > 0")
@dlt.expect("post_id", "post_id IS NOT NULL")
@dlt.expect("post_id>0", "post_id > 0")

def silver_comments():
    return(
        spark.readStream.table("LIVE.bronze_comments")
        .select("*")
        .withColumn("comment_text", F.trim(F.col("comment_text")))
        .withColumn("is_image", F.col("is_image")).cast("int")
        .withColumn("status", F.when(F.col("status") == 'n/d', None).otherwise(F.col("status")))
    )

dlt.create_streaming_table(
    name = "silver_comments",
    comment = "SCD Type 2 for comments",
    table_properties = {"schema" : "silver"}
)


dlt.apply_changes(
    target = "silver_comments",
    source = "silver_comments_clean",
    keys = "comment_id",
    sequence_by = "created_at_time",
    stored_as_scd_type = "2"
)




@dlt.table(
    name = "silver_reactions_clean",
    comment = "silver table for reactions",
    table_properties = {"schema" : "silver"}
)



@dlt.expect("reacted_at_time", "reacted_at_time IS NOT NULL")
@dlt.expect("account_id", "account_id IS NOT NULL")
@dlt.expect("account_id>0", "account_id > 0")
@dlt.expect("post_id", "post_id IS NOT NULL")
@dlt.expect("post_id>0", "post_id > 0")
def silver_reactions():
    return(
        spark.readStream.table("LIVE.bronze_reactions")
        .select("*")
        .withColumn("reaction_type", F.when(F.col("reaction_type") == 'n/d',None).otherwise(F.col("reaction_type")))
    )


dlt.create_streaming_table(
    name = "silver_reactions",
    comment = "SCD Type 2 for reactions",
    table_properties = {"schema" : "silver"}
)


dlt.apply_changes(
    target = "silver_reactions",
    source = "silver_reactions_clean",
    keys = "reaction_id",
    sequence_by = "reacted_at_time",
    stored_as_scd_type = "2"
)









'''






















