from pyspark.sql import functions as F
import dlt

##### account_user #####
@dlt.table(
    name = "silver_account_user_clean",
    table_properties = {"schema" : "silver"},
    comment = "silver table for account_user"
)

@dlt.expect("is_group","is_group IS NOT NULL")
@dlt.expect_or_drop("account_id","account_id IS NOT NULL")
@dlt.expect_or_fail("account_id", "account_id <= 0 ")
@dlt.expect_or_drop("login","login IS NOT NULL")
@dlt.expect_or_drop("password","password IS NOT NULL")
def silver_account_user():
    return (
        spark.readStream.table("LIVE.bronze_account_user")
        .select("*") # check 
        .withColumn("is_group", F.col("is_group")).cast("int")
        .withColumn("account_name", F.trim(F.col("account_name")))
        .withColumn("first_name", F.trim(F.col("first_name")))
        .withColumn("last_name", F.trim(F.col("last_name")))
        .withColumn("display_name", F.trim(F.col("display_name")))
        .withColumn("first_name", F.when(F.col("first_name") == 'n/d', None).otherwise(F.col("first_name"))) # check this 
        .withColumn("last_name", F.when(F.col("last_name") == 'n/d', None).otherwise(F.col("last_name")))
        .withColumn("display_name", F.when(F.col("display_name") = 'n/d', None).othwerise(F.col("display_name")))
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
    keys = "account_id",
    sequence_by = "account_id"
    stored_as_scd_type = "2"
)




### follow_relationship

dlt.table(
    name = "silver_follow_relationship_clean",
    comment = "silver table for follow_relationship",
    table_properties = {"schema" : "silver"}
)

@dlt.expect("self_following","follower_account_id <> followed_account_id")
@dlt.expect("date_null", "followed_at_time IS NOT NULL")

def silver_follow_relationship():
    return(
        spark.readStream.table("LIVE.bronze_follow_relationship")
        .select("*")
    )


dlt.create_streaming_table(
    name = "silver_follow_relationship",
    comment = "SCD Type 2 follow_relationship",
    table_properties = {"schema" : "silver"}
)


dlt.apply_changes(
    target = "silver_follow_relationship",
    source = "silver_follow_relationship_clean",
    keys = ["follower_account_id", "followed_account_id"],
    sequence_by = "followed_at_time_id" # -> check,
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

@dlt.expectations("advertiser_id", "advertiser_id IS NOT NULL")
@dlt.expectations("billing_account_code", "billing_account_code IS NOT NULL")
@dlt.expectations("billing_status", "billing_status IS NOT NULL")

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
    sequence = "advertiser_id" # check 
)
























