# Social media Data Engineering Project

### 1. Global Objective
The goal of the project was to integrate data from a Social Media application stored in ** Azure SQL Database** with semi-structured JSON files containing detailed user information.
An ETL data pipeline was implemented to ingest, transform, and load the data into a Data Lake following the **Medallion Architecture (Bronze, Silver, Gold)**.

Two different pipeline implementations were developed:
- Databricks Workflows Jobs – traditional task-based ETL orchestration
- Delta Live Tables (DLT) – a declarative approach

### 2. Technology stack stack:
- Databricks
- Apache Spark (PySpark & Spark SQL)
- Azure SQL Database
- Delta Live Tables (DLT)
- Delta Lake
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Azure Key Vault


### 3. Entities Available in the Azure SQL Database
[Database schema diagram]
1. Account_users - Stores basic information about users of the social media app
2. Time - Time table
3. Follow relationship - Represents follower relationships between users (follows)
4. Advertisers - Contains information about companies purchasing advertisements in the social media app
5. Advertisements - Basic data about displayed advertisements
6. Posts - Contains information about posts created by users
7. Post_media - Links posts with associated media files such as images, videos, or GIFs
8. Hashtags - Table containing used/created hashtags
9. Post_hashtags - Links posts with hashtags
10. Comments - Stores user comments in posts
11. Reactions - Contains all users reactions (likes, etc.)


### 4. Repository Structure

```
Social_media_Data_Engineering_Project/
│
├── README.md
│
├──workflow_job/
|  ├── bronze_layer_ingestion.py
│  ├── silver_layer_transformations.py
│  └── gold_layer_analytics.py
│
│
├──DLT/
|  ├── bronze_layer_ingestion_DLT.py
│  ├── silver_layer_transformations_DLT.py
│  └── gold_layer_analytics_DLT.py
│
│──MLFLOW/
|  ├── ML_model.py
|
```


### 5.Azure Databricks Environment Configuration
#### Resource Group: sm-databricks-rg
[screen of Azure configuration]

1. Create Azure Databricks Workspace "sm-databricks-ws"
 - Create a new Azure Databricks workspace with the name sm-databricks-ws.

2. Create SQL Azure Server and Azure SQL Database
- Server: Create an Azure SQL logical server. Its fully qualified domain name (FQDN) will be sm-srv.database.windows.net. 
- Database: On the server above, create an Azure SQL Database named sm-db.

3. Connect Workspace to Metastore for Cluster-Unity Catalog Integration
- Configure the newly created Databricks workspace (sm-databricks-ws) to connect to an existing, external Hive metastore. This enables clusters within the workspace to leverage Unity Catalog for centralized governance.

4. Configure Access to Cloud Storage
- 4.1. Create an Access Connector for Azure Databricks
- Create an Access Connector for Azure Databricks resource named sm-databricks-ext-ac. This provides a managed identity for Azure Databricks.

4.2. Create a Storage Account (Azure Data Lake Storage Gen2)
- Provision a new Azure Data Lake Storage Gen2 (ADLS Gen2) account.

4.3. Assign Storage Account Role to the Access Connector
- On the ADLS Gen2 storage account, assign the Storage Blob Data Contributor role to the Access Connector's managed identity (sm-databricks-ext-ac). This grants read, write, and delete permissions to the data in the storage account.

4.4. Create a Storage Credential in Unity Catalog
- Within the Unity Catalog interface (Catalog > Credentials), create a new Storage Credential.

Name: sm_databricks_ext_sc
Type: Azure Managed Identity

Azure Managed Identity: Use the Managed Identity from the Access Connector sm-databricks-ext-ac. This credential securely references the storage account from within Unity Catalog.

### 6. Azure Key Vault 
When ingesting data from Azure SQL Database via JDBC in ETL code, sensitive information such as passwords, usernames, table names, and URLs are not hardcoded. I utilize a secret scope integrated with Azure Key Vault to store safely these cretentials
[screen showing created secrets]

### 7. Implementation of Databricks Workflow Job

##### 7.1. Bronze Layer
As previously mentioned, data is ingested via JDBC using Azure Key Vault, which ensures that sensitive information are not exposed directly in the code. Implementation is written in **PySpark** The Bronze Layer implements a full load apporach, each run overwrites the existing data withoud maintaining historical versions. 

Code snippet responsible for extracting data from the account_user table:
```
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
```
During extraction an ingest_time column is added ot each table, capturing the timestamp of when the data was pulled from the source system.

Looking for duplicated rows and saving table to Bronze Layer (schema):
```
w = Window.partitionBy("account_id").orderBy(F.col("ingest_time").desc())
stg = stg.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
stg.writeTo("content_job.bronze.account_user").createOrReplace()
```

Create of the sha_key column, generated by hash function, used to detect changes and new rows for **SCD Type II** implementation in the Silver Layer.
```
cols = stg.columns
tracked_cols = [col for col in cols if col not in ["account_id","ingest_time"]]#cols.remove("account_id","ingest_time")
df_sha256 = stg.withColumn("sha_key", F.sha2(F.concat_ws('|',*[F.col(col).cast(StringType()) for col in tracked_cols]),256))
df_sha256.writeTo("content_job.temp.df_sha256_account_user").createOrReplace()
```

##### 7.2. JSON files handle
[screen of azure storage]
The JSON files contain detailed user information, with each file storing a nested structure for several thousand users. The data pipeline employs a StructType schema to extract specific fields from these nested structures.
```
json_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("friendsCount", IntegerType(), True),
    StructField("favouritesCount", IntegerType(), True),
    StructField("listedCount", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("rawDescription", StringType(), True),
    StructField("accountMetadata", StructType([
        StructField("accountAge", StructType([
            StructField("createdYear", StringType(), True),
            StructField("createdMonth", StringType(), True),
            StructField("accountAgeCategory", StringType(), True)
        ])),
        StructField("verificationStatus", StructType([
            StructField("isVerified", IntegerType(), True),
            StructField("verificationConfidence", StringType(), True)
        ]))
    ])),
    StructField("analyticsFlags", StructType([
        StructField("potentialBot", IntegerType(), True),
        StructField("potentialInfluencer", IntegerType(), True)
    ])),
    StructField("profileAnalysis", StructType([
        StructField("profileCompletenessScore", DoubleType(), True)
    ])),
    StructField("networkFeatures", StructType([
    StructField("networkType", StringType(), True),
    StructField("influenceIndicators", StructType([
        StructField("influenceScore", DoubleType(), True)
    ]))
]))
])
```

Unlike other tables in Bronze Layer, account_details uses incremental data load with **AutoLoader**.

```
def bronze_account_details():
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(json_schema)
            .option("multiline","true")
            .load("/Volumes/content/landing/json_files_data/*.json")
            .select(
                "*",
                F.current_timestamp().alias("ingest_time")
))
```

##### 7.3. Silver Layer
As mentioned earlier, the Silver layer implements SCD Type II to maintain a complete historical view of the data, providing visibility into which records have been deleted,along with their deletion date and which are inactive, along with their expiration date. The Slowly Changing Dimension mechanism has been implemented using MERGE INTO statements in Spark SQL.
Code uses previously created sha_key column to detect changes in records, and uses id columns to verify whether new records have appeared in Bronze layer, which don't exist yet in Silver Layer.

```
MERGE INTO content_job.silver.account_user tgt 
USING (

      --INSERT COMPLETELY NEW RECORDS 
      SELECT src_null.*, NULL AS mergeKey, 'INSERT' AS action FROM content_job.temp.df_sha256_account_user src_null
      LEFT JOIN content_job.silver.account_user tgt
      ON src_null.account_id = tgt.account_id AND tgt.is_current = true
      WHERE tgt.account_id IS NULL OR src_null.sha_key <> tgt.sha_key

      UNION ALL

      --UPDATE RECORS WHICH HAD CHANGED IN SOURCE
      SELECT src.*, src.account_id AS mergeKey, 'UPDATE' AS action FROM content_job.temp.df_sha256_account_user src
      JOIN content_job.silver.account_user tgt ON src.account_id = tgt.account_id 
      WHERE tgt.sha_key <> src.sha_key AND tgt.is_current = True
      
      UNION ALL

      --- DELETED ROWS
      SELECT tgt.account_id, tgt.account_name, tgt.is_group, tgt.first_name, tgt.last_name, tgt.display_name, tgt.profile_url, tgt.profile_image_storage, 
      tgt.profile_baner_storage, tgt.login, tgt.password, tgt.second_mail, tgt.valid_from, tgt.sha_key, tgt.account_id AS mergeKey, 'DELETE' AS action
      FROM content_job.silver.account_user tgt 
      LEFT JOIN content_job.temp.df_sha256_account_user src ON tgt.account_id = src.account_id
      WHERE src.account_id IS NULL AND tgt.is_current = true
      
      ) src
      ON tgt.account_id = src.mergeKey AND tgt.is_current  = True

      WHEN MATCHED AND tgt.sha_key <> src.sha_key OR src.action = 'DELETE' THEN UPDATE 
      SET 
      tgt.valid_to = src.ingest_time,
      tgt.is_current = false

      WHEN NOT MATCHED THEN
      INSERT (...)

      VALUES(...)
```













