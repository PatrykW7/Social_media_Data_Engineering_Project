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

spark.sql(sql_code1)


# closing the outdated records 
# This part of code will run even with empty target table 
sql_code2 = """
MERGE INTO content_job.silver.account_user_scd_type2  AS tgt
USING (
    SELECT *, true AS is_current FROM content_job.temp.df_sha256_account_user) src 
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
USING content_job.temp.df_sha256_account_user AS src
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


############# ACCOUNT DETAILS - JSON ############


sql_code4 = """
CREATE TABLE IF NOT EXISTS content_job.silver.silver_account_details (
    userId STRING,
    account_creation_year_month STRING,
    accountAgeCategory STRING,
    isVerified BOOLEAN,
    verificationConfidence DOUBLE,
    potentialBot BOOLEAN,
    potentialInfluencer BOOLEAN,
    friendsCount INT,
    listedCount INT,
    location STRING,
    rawDescription STRING,
    profileCompletenessScore DOUBLE,
    networkType STRING,
    sha_key STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
    
    
)
USING DELTA;

"""

spark.sql(sql_code4)


# updating old records
sql_code5 = """
MERGE INTO content_job.silver.silver_account_details tgt    
USING content_job.temp.df_sha256_account_details AS src
    ON tgt.userId = src.userId 
    AND tgt.is_current = true
    WHEN MATCHED AND tgt.sha_key <> src.sha_key
    THEN UPDATE SET tgt.valid_to = src.ingest_time,
         tgt.is_current = false
"""

spark.sql(sql_code5)




sql_code6 = """
MERGE INTO content_job.silver.silver_account_details tgt
USING content_job.temp.df_sha256_account_details AS src
    ON tgt.userId = src.userId
    AND tgt.is_current = true
    WHEN NOT MATCHED THEN
    INSERT (
        userId,
        account_creation_year_month,
        accountAgeCategory,
        isVerified,
        verificationConfidence,
        potentialBot,
        potentialInfluencer,
        friendsCount,
        listedCount,
        location,
        rawDescription,
        profileCompletenessScore,
        networkType,
        sha_key,
        valid_from,
        valid_to,
        is_current
    ) 
    VALUES (
        src.userId,
        src.account_creation_year_month,
        src.accountAgeCategory,
        src.isVerified,
        src.verificationConfidence,
        src.potentialBot,
        src.potentialInfluencer,
        src.friendsCount,
        src.listedCount,
        src.location,
        src.rawDescription,
        src.profileCompletenessScore,
        src.networkType,
        src.sha_key,
        src.ingest_time,
        to_timestamp('9999-12-31 23:59:59'),
        true
        )

"""


spark.sql(sql_code6)

























