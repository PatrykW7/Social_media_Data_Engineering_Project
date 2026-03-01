sql_code = """
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

spark.sql(sql_code)


sql_code = """
MERGE INTO content_job.silver.account_user tgt 
USING (
      --INSERT COMPLETELY NEW RECORDS 
      SELECT src_null.*, NULL AS mergeKey FROM content_job.temp.df_sha256_account_user src_null
      LEFT JOIN content_job.silver.account_user tgt
      ON src_null.account_id = tgt.account_id AND tgt.is_current = True
      WHERE tgt.account_id IS NULL

      UNION ALL

      --UPDATE RECORS WHICH HAD CHANGED IN SOURCE
      SELECT src.*, src.account_id AS mergeKey FROM content_job.temp.df_sha256_account_user src
      JOIN content_job.silver.account_user tgt ON src.account_id = tgt.account_id 
      WHERE tgt.sha_key <> src.sha_key AND tgt.is_current = True
      
      ) src
      ON tgt.account_id = src.mergeKey AND tgt.is_current  = True

      WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN UPDATE 
      SET 
      tgt.valid_to = src.ingest_time,
      tgt.is_current = false

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

spark.sql(sql_code)




############# ACCOUNT DETAILS - JSON ############


sql_code = """
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


spark.sql(sql_code)


sql_code = """
MERGE INTO content_job.silver.account_details AS tgt
USING (
    -- COMPLETELY NEW ROWS TO INSERT 
    SELECT src_null.*, NULL AS mergeKey 
    FROM content_job.temp.df_sha256_account_details AS src_null
    LEFT JOIN content_job.silver.account_details tgt ON src_null.userId = tgt.userId 
    WHERE tgt.userId IS NULL

    UNION ALL

    -- ROWS WHICH HAD CHANGED 
    SELECT src.*, src.userId AS mergeKey 
    FROM content_job.temp.df_sha256_account_details AS src
    JOIN content_job.silver.account_details tgt ON src.userId = tgt.userId
    WHERE tgt.is_current = true AND src.sha_key <> tgt.sha_key  
    ) src

    ON tgt.userId = src.mergeKey AND tgt.is_current = True

    WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN UPDATE 
    SET 
        tgt.is_current = false,
        tgt.valid_to = src.ingest_time

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

spark.sql(sql_code)


######################## TIME ######################




sql_code = """
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

spark.sql(sql_code)


sql_code = """
MERGE INTO content_job.silver.time tgt 
USING (
       -- INSERT COMPLETELY NEW ROWS
       SELECT src_null.*, NULL AS mergeKey
       FROM content_job.temp.df_sha256_time src_null
       LEFT JOIN content_job.silver.time tgt ON src_null.time_id = tgt.time_id 
       WHERE tgt.time_id IS NULL

       UNION ALL
    
       -- UPDATE ROWS THAT HAD CHANGED
       SELECT src.*, src.time_id AS mergeKey
       FROM content_job.temp.df_sha256_time src 
       JOIN content_job.silver.time tgt ON src.time_id = tgt.time_id
       WHERE tgt.is_current = true AND tgt.sha_key <> src.sha_key 

) src 
ON src.mergeKey = tgt.time_id AND tgt.is_current = true

WHEN MATCHED AND src.sha_key <> tgt.sha_key THEN UPDATE SET 
    tgt.is_current = false,
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

spark.sql(sql_code)



sql_code = """
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

spark.sql(sql_code)

sql_code = """
MERGE INTO content_job.silver.follow_relationship tgt
USING (
    SELECT src_null.*, NULL AS mergeKey
    FROM content_job.temp.df_sha256_follow_relationship src_null
    LEFT JOIN content_job.silver.follow_relationship tgt 
    ON src_null.follower_account_id = tgt.follower_account_id 
    AND src_null.followed_account_id = tgt.followed_account_id
    WHERE CONCAT(tgt.follower_account_id, tgt.followed_account_id) IS NULL
    
    UNION ALL

    SELECT src.*, CONCAT(src.follower_account_id, src.followed_account_id) AS mergeKey
    FROM content_job.temp.df_sha256_follow_relationship src
    JOIN content_job.silver.follow_relationship tgt ON 
    CONCAT(src.follower_account_id, src.followed_account_id) = CONCAT(tgt.follower_account_id, tgt.followed_account_id)
    WHERE tgt.is_current AND tgt.sha_key <> src.sha_key
    
) src

ON src.mergeKey = CONCAT(tgt.follower_account_id, tgt.followed_account_id) AND tgt.is_current = true

WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN UPDATE 
    SET 
    tgt.valid_to = src.ingest_time,
    tgt.is_current = false

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


spark.sql(sql_code)



### ADVERTISERS


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




sql_code13 = """
CREATE TABLE IF NOT EXISTS content_job.silver.advertisements(
    advertisement_id INT,
    advertiser_id INT,
    ad_name VARCHAR(50),
    ad_title VARCHAR(100),
    price_USD FLOAT,
    pricing_model STRING,
    start_at INT,
    end_at INT,
    ad_text STRING,
    landing_url VARCHAR(150),
    status VARCHAR(50),
    Euro_price DOUBLE,
    created_at_time INT,
    sha_key STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN

)
USING DELTA;


"""

spark.sql(sql_code13)


sql_code14 = """
MERGE INTO content_job.silver.advertisements tgt
USING content_job.temp.df_sha256_advertisements src
ON tgt.advertisement_id = src.advertisement_id
AND tgt.is_current = True

WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN UPDATE 
SET tgt.is_current = False,
    tgt.valid_to = src.ingest_time

WHEN NOT MATCHED THEN
INSERT (
        advertisement_id,
        advertiser_id,
        ad_name,
        ad_title,
        price_USD,
        pricing_model,
        start_at,
        end_at,
        ad_text,
        landing_url,
        status,
        Euro_price,
        created_at_time,
        sha_key,
        valid_from,
        valid_to,       
        is_current
        )


VALUES (
        src.advertisement_id,
        src.advertiser_id,
        src.ad_name,
        src.ad_title,
        src.price_USD,
        src.pricing_model,
        src.start_at,
        src.end_at,
        src.ad_text,
        src.landing_url,
        src.status,
        src.Euro_price,
        src.created_at_time,
        src.sha_key,
        src.ingest_time,
        to_timestamp('9999-12-31 23:59:59'),
        True
        )


"""

spark.sql(sql_code14)


####### POSTS######
sql_code15 = """
CREATE TABLE IF NOT EXISTS content_job.silver.posts(
    post_id INT,
    post_text VARCHAR(300),
    author_id INT,
    visibility VARCHAR(20),
    created_at_time INT,
    reply_to_pos_time INT,
    language_code VARCHAR(10),
    advertisement_id INT,
    is_deleted BOOLEAN,
    sha_key STRING,
    valid_from DATETIME,
    valid_to DATETIME,
    is_current BOOLEAN

)

USING DELTA;

"""

spark.sql(sql_code15)


sql_code16 = """
MERGE INTO content_job.silver.posts tgt
USING content_job.temp.df_sha256_posts src
ON tgt.post_id = src.post_id 
AND tgt.is_current = True

WHEN 




"""

