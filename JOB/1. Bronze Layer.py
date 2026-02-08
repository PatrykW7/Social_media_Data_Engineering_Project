from pyspark.sql import functions as F 

def bronze_account_user():
    return (
        spark.read
        .format("jdbc") 
        .option("url", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-jdbc")) 
        .option("username", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-dblog")) 
        .option("password", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-secret")) 
        .option("dbtable", dbutils.secrets.get(scope="sm-secret-scope", key = "social-media-project-db-tab-acc-users")) 
        .load()
    )

if True:
    bronze_account_user().writeTo("content_job.bronze.bronze_account_user").createOrReplace()
    print("Udalo sie")
else:
    print("PASS")