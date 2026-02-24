
sql_code1 = """
CREATE TABLE IF NOT EXISTS content_job.silver.account_user (
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


sql_code2 = """
MERGE INTO content_job.silver.account_user  AS tgt
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
MERGE INTO content_job.silver.account_user AS tgt
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
CREATE TABLE IF NOT EXISTS content_job.silver.account_details (
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
MERGE INTO content_job.silver.account_details tgt    
USING content_job.temp.df_sha256_account_details AS src
    ON tgt.userId = src.userId 
    AND tgt.is_current = true
    WHEN MATCHED AND tgt.sha_key <> src.sha_key
    THEN UPDATE SET tgt.valid_to = src.ingest_time,
         tgt.is_current = false
"""

spark.sql(sql_code5)




sql_code6 = """
MERGE INTO content_job.silver.account_details tgt
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


######################## TIME ######################



sql_code7 = """
CREATE TABLE IF NOT EXISTS content_job.silver.time(
time_id INT,
date DATE,
year INT,
month INT,
day INT,
quarter INT,
week_number INT,
half_year INT,
sha_key STRING,
valid_from TIMESTAMP,
valid_to TIMESTAMP,
is_current BOOLEAN
)

USING DELTA;


"""

spark.sql(sql_code7)



sql_code8 = """
MERGE INTO content_job.silver.time tgt
USING content_job.temp.df_sha256_time src
ON tgt.time_id = src.time_id 
   AND tgt.is_current = True

WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN UPDATE
    SET 
        tgt.is_current = False,
        tgt.valid_to = src.ingest_time

WHEN NOT MATCHED THEN
     INSERT 
     (
         time_id,
         date,
         year,
         month,
         day,
         quarter,
         week_number,
         half_year,
         sha_key,
         valid_from,
         valid_to,
         is_current)
     VALUES 
        (src.time_id,
        src.date,
        src.year,
        src.month,
        src.day,
        src.quarter,
        src.week_number,
        src.half_year,
        src.sha_key,
        src.ingest_time,
        to_timestamp('9999-12-31 23:59:59'),
        true
    
    )


"""

spark.sql(sql_code8)





sql_code9 = """
CREATE TABLE IF NOT EXISTS content_job.silver.follow_relationship (
follower_account_id INT,
followed_account_id INT,
followed_at_time_id INT,
status VARCHAR(150),
source VARCHAR(150),
sha_key STRING,
valid_from TIMESTAMP,
valid_to TIMESTAMP,
is_current BOOLEAN    

)

USING DELTA;


"""

spark.sql(sql_code9)


sql_code10 = """
MERGE INTO content_job.silver.follow_relationship tgt
USING content_job.temp.df_sha256_follow_relationship src
ON tgt.follower_account_id = src.follower_account_id 
AND tgt.followed_account_id = src.followed_account_id 
AND tgt.is_current = True

WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN UPDATE 
SET tgt.is_current = False,
    tgt.valid_to = src.ingest_time

WHEN NOT MATCHED THEN
    INSERT (
        follower_account_id,
        followed_account_id,
        followed_at_time_id,
        status,
        source,
        sha_key,
        valid_from,
        valid_to,
        is_current
    )
    VALUES (
        src.follower_account_id,
        src.followed_account_id,
        src.followed_at_time_id,
        src.status,
        src.source,
        src.sha_key,
        src.ingest_time,
        to_timestamp('9999-12-31 23:59:59'),
        true
        )

"""


spark.sql(sql_code10)


sql_code11 = """
CREATE TABLE IF NOT EXISTS content_job.silver.advertisers(
    advertiser_id INT,
    advertiser_name VARCHAR(250),
    destination_group VARCHAR(150),
    billing_account_code VARCHAR(50),
    billing_status VARCHAR(50),
    sha_key STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN

)

USING DELTA;

"""

spark.sql(sql_code11)


sql_code12 = """
MERGE INTO content_job.silver.advertisers tgt
USING content_job.temp.df_sha256_advertisers src
ON tgt.advertiser_id = src.advertiser_id
AND tgt.is_current = True

WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN UPDATE
SET tgt.is_current = False,
    tgt.valid_to = src.ingest_time


WHEN NOT MATCHED THEN 
    INSERT (
            advertiser_id,
            advertiser_name,
            destination_group,
            billing_account_code,
            billing_status,
            sha_key,
            valid_from,
            valid_to,
            is_current
            )
    VALUES (
            src.advertiser_id,
            src.advertiser_name,
            src.destination_group,
            src.billing_account_code,
            src.billing_status,
            src.sha_key,
            src.ingest_time,
            to_timestamp('9999-12-31 23:59:59'),
            true
            )

"""

spark.sql(sql_code12)





