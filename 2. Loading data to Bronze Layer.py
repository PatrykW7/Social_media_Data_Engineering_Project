# Databricks notebook source
from pyspark.sql import functions as F
import dlt

# Creating bronze table for account_user
@dlt.table(name = "bronze_account_user",
          table_properties = {"schema" : "bronze"},
          comment = "Bronze table for account_user")
def bronze_account_user():
    return(
        spark.read(
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-jdbc"))
        .option("dbtable", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-tab-acc-users"))
        .option("user", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-dblog"))
        .option("password", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-secret"))
        .load()
        .select(
                "*",
                F.current_timestamp().alias("ingest_time")
        
        )
    )
)
    

@dlt.pipeline(
    name => "time",
    comment = "table with time"
)
def time():
    return(
        spark.read(
            .format("jdbc")
            .option("url", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-jdbc"))
            .option("dbtable", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-tab-time"))
            .option("user", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-dblog"))
            .option("password", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-secret"))
            .load()
            .select(
                "*",
                F.current_timestamp().alias("ingest_time")
            )
        )
    )


@dlt.table(
    name="bronze_follow_relationship",
    table_properties= {
        "schema":"bronze"
    },
    comment = "bronze table for follow_relationship"
)
def bronze_follow_relationship():
    return (spark.read(
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-jdbc"))
        .option("dbtable", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-tab-follow-relationship"))
        .option("user", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-dblog"))
        .option("password", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-secret"))
        .load()
        .select(
            "*",
            F.current_timestamp().alias("ingest_time")
            )

        )
)


@dlt.table(
    name = "bronze_advertisers",
    table_properties = {
        "schema":"bronze"
    },
    comment = "bronze table for advertisers"
)
def bronze_advertisers():
    return (spark.read(
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-jdbc"))
        .option("dbtable", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-tab-advertisers"))
        .option("user", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-dblog"))
        .option("password", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-secret"))
        .load()
        .select(
            "*",
            F.current_timestamp().alias("ingest_data")
        )
    ))


@dlt.table(
    name = "bronze_advertisements"
    table_properties = {
        "schema" : "bronze"
    },
    comments = "bronze table for advertisements"
)
def bronze_advertisements():
    return (spark.read(
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-jdbc"))
        .option("dbtable", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-tab-advertisements"))
        .option("user", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-dblog"))
        .option("password", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-secret"))
        .load()
        .select(
            "*",
            F.current_timestamp().alias("ingest_time")
        )
    ))



@dlt.table(
    name:"bronze_posts",
    table_properties = {
        "schema" : "bronze"
    },
    comment = "bronze table for posts"
)
def bronze_posts():
    return (spark.read(
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-jdbc"))
        .option("dbtable", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-tab-posts"))
        .option("user", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-dblog"))
        .option("password", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-secret"))
        .load()
        .select(
            "*",
            F.current_timestamp().alias("ingest_time")
        )
    ))




@dlt.table(
    name = "bronze_post_media",
    table_properties = {
        schema = "bronze"
    },
    comment = "bronze table for post_media"
)
def bronze_post_media():
    return (spark.read(
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-jdbc"))
        .option("dbtable", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-tab-post-media"))
        .option("user", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-dblog"))
        .option("password", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-secret"))
        .load()
        .select(
            "*",
            F.current_timestamp().alias("ingest_time")
        )
    ))





@dlt.table(
    name = "bronze_hashtags",
    table_properties = {
        "schema" : "bronze"
    },
    comment = "bronze table for hashtags"
)
def bronze_hashtags():
    return (spark.read(
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-jdbc"))
        .option("dbtable", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-tab-hashtags"))
        .option("user", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-dblog"))
        .option("password", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-secret"))
        .load()
        .select(
            "*",
            F.current_timestamp().alias("ingest_time")
        )
))



@dlt.table(
    name = "bronze_post_hashtags",
    table_properties = {
        "schema" : "bronze"
    },
    comments = "bronze table for post_hashtags"
)
def bronze_post_hashtags():
    return (spark.read(
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-jdbc"))
        .option("dbtable", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-tab-post-hashtags"))
        .option("user", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-dblog"))
        .option("password", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-secret"))
        .load()
        .select(
            "*",
            F.current_timestamp().alias("ingest_time")
        )
    ))



@dlt.table(
    name = "bronze_comments",
    table_properties = {
        "schema":"bronze"
    },
    comments = "bronze table for comments"
)
def bronze_comments():
    return (spark.read(
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-jdbc"))
        .option("dbtable", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-tab-comments"))
        .option("user", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-dblog"))
        .option("password", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-secret"))
        .load()
        .select(
            "*",
            F.current_timestamp().alias("ingest_time")
        )
    ))



@dlt.table(
    name = "bronze_reactions",
    table_properties = {
        "schema":"bronze"
    },
    comments = "bronze table for reactions"
)
def bronze_reactions():
    return (spark.read(
        .format("jdbc")
        .option("url", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-jdbc"))
        .option("dbtable", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-db-tab-reactions"))
        .option("user", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-dblog"))
        .option("password", dbutils.secrets.get(scope=>"sm-secret-scope", key => "social-media-project-secret"))
        .load()
        .select(
            "*",
            F.current_timestamp().alias("ingest_time")
        )
    ))





















































