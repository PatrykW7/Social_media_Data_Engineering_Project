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
sql_code = """
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

spark.sql(sql_code)


sql_code = """
MERGE INTO content_job.silver.advertisers tgt 
USING (
       -- INSERT COMPLETELY NEW ROWS
       SELECT src_null.* , NULL AS mergeKey FROM content_job.temp.df_sha256_advertisers src_null
       LEFT JOIN content_job.silver.advertisers tgt ON tgt.advertiser_id = src_null.advertiser_id
       WHERE tgt.advertiser_id IS NULL
       
       UNION ALL

       -- ROWS THAT HAD CHANGED 
       SELECT src.*, src.advertiser_id AS mergeKey FROM content_job.temp.df_sha256_advertisers src
       JOIN content_job.silver.advertisers tgt ON tgt.advertiser_id = src.advertiser_id 
       WHERE tgt.is_current = true AND tgt.sha_key <> src.sha_key
       
       ) src

ON tgt.advertiser_id = src.mergeKey AND tgt.is_current = true

WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN UPDATE 
SET 
   tgt.is_current = false,
   tgt.valid_to = src.ingest_time

WHEN NOT MATCHED THEN
INSERT(
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



spark.sql(sql_code)


####### ADVERTISEMENTS

sql_code = """
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

spark.sql(sql_code)


sql_code = """
MERGE INTO content_job.silver.advertisements tgt
USING (
       --- INSERT COMPLETELY NEW ROWS
       SELECT src_null.*, NULL AS mergeKey FROM content_job.temp.df_sha256_advertisements src_null\
       LEFT JOIN content_job.silver.advertisements tgt ON tgt.advertisement_id = src_null.advertisement_id 
       WHERE tgt.advertisement_id IS NULL

       UNION ALL

       -- ROWS THAT HAD CHANGED
       SELECT src.*, src.advertisement_id AS mergeKey FROM content_job.temp.df_sha256_advertisements src
       JOIN content_job.silver.advertisements tgt ON src.advertisement_id = tgt.advertisement_id
       WHERE tgt.is_current = true AND tgt.sha_key <> src.sha_key 

       ) src

ON tgt.advertisement_id = src.mergeKey AND tgt.is_current = true

WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN UPDATE 
SET 
   tgt.valid_to = src.ingest_time,
   tgt.is_current = false

WHEN NOT MATCHED THEN 
INSERT(
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
VALUES(
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
       true
       )

"""







####### POSTS######
sql_code = """
CREATE TABLE IF NOT EXISTS content_job.silver.posts(
    post_id INT,
    post_text STRING,
    author_id INT,
    visibility VARCHAR(200),
    created_at_time INT,
    reply_to_post_time INT,
    language_code VARCHAR(100),
    advertisement_id INT,
    is_deleted BOOLEAN,
    sha_key STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN

)

USING DELTA;

"""

spark.sql(sql_code)



sql_code = """
MERGE INTO content_job.silver.posts tgt
USING (
       -- INSERT COMPLETELTY NEW ROWS
       SELECT src_null.*, NULL AS mergeKey FROM content_job.temp.df_sha256_posts src_null
       LEFT JOIN content_job.silver.posts tgt ON src_null.post_id = tgt.post_id
       WHERE tgt.post_id IS NULL

       -- ROWS THAT HAD CHANGED IN THE PAST
       UNION ALL
       SELECT src.*, src.post_id as mergeKey FROM content_job.temp.df_sha256_posts src
       JOIN content_job.silver.posts tgt ON tgt.post_id = src.post_id
       WHERE tgt.is_current = true AND src.sha_key <> tgt.sha_key
       
       ) src
ON tgt.post_id = src.mergeKey AND tgt.is_current = true 

WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN UPDATE
SET
   tgt.is_current = false,
   tgt.valid_to = src.ingest_time

WHEN NOT MATCHED THEN 
INSERT(
       post_id,
       post_text,
       author_id,
       visibility,
       created_at_time,
       reply_to_post_time,
       language_code,
       advertisement_id,
       is_deleted,
       sha_key,
       valid_from,
       valid_to,
       is_current
       )
VALUES(
       src.post_id,
       src.post_text,
       src.author_id,
       src.visibility,
       src.created_at_time,
       src.reply_to_post_time,
       src.language_code,
       src.advertisement_id,
       src.is_deleted,
       src.sha_key,
       src.ingest_time,
       to_timestamp('9999-12-31 23:59:59'),   
       true
       )

"""


spark.sql(sql_code)


sql_code = """
CREATE TABLE IF NOT EXISTS content_job.silver.post_media (
       media_id INT,
       post_id INT,
       media_type VARCHAR(50),
       media_storage VARCHAR(70),
       duration_sec DOUBLE,
       sha_key STRING,
       valid_from TIMESTAMP,
       valid_to TIMESTAMP,
       is_current BOOLEAN
)

USING DELTA;

"""

spark.sql(sql_code)

#### CHECK TOMORROW DOES IT WORK CORRECTLY 
sql_code = """
MERGE INTO content_job.silver.post_media tgt
USING (
       -- COMPLETELY NEW ROWS
       SELECT src_null.*, NULL as mergeKey FROM content_job.temp.df_sha256_post_media src_null
       LEFT JOIN content_job.silver.post_media tgt ON CONCAT(src_null.media_id, src_null.post_id) = CONCAT(tgt.media_id, tgt.post_id)
       WHERE CONCAT(tgt.media_id, tgt.post_id) IS NULL
       
       UNION ALL

       -- ROWS THAT HAD CHANGED
       SELECT src.*, CONCAT(src.media_id, src.post_id) as mergeKey FROM content_job.temp.df_sha256_post_media src
       JOIN content_job.silver.post_media tgt ON CONCAT(src.media_id, src.post_id) = CONCAT(tgt.media_id, tgt.post_id)
       WHERE tgt.is_current AND tgt.sha_key <> src.sha_key

       ) src

ON CONCAT(tgt.media_id, tgt.post_id) = CONCAT(src.media_id, src.post_id) AND tgt.is_current = true

WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN UPDATE 
SET 
   tgt.valid_to = src.ingest_time,
   tgt.is_current = false
 

WHEN NOT MATCHED THEN 
INSERT(
       media_id,
       post_id,
       media_type,
       media_storage,
       duration_sec,
       sha_key,
       valid_from,
       valid_to,
       is_current
       )
VALUES(
       src.media_id,
       src.post_id,
       src.media_type,
       src.media_storage,
       src.duration_sec,
       src.sha_key,
       src.ingest_time,
       to_timestamp('9999-12-31 23:59:59'),
       true)

"""

spark.sql(sql_code)


####### HASHTAGS ######


sql_code = """
CREATE TABLE IF NOT EXISTS content_job.silver.hashtags(
    hashtag_id INT,
    tag_text VARCHAR(25),
    first_use_time INT,
    sha_key STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
    )

USING DELTA;

"""

spark.sql(sql_code)


sql_code = """
MERGE INTO content_job.silver.hashtags tgt
USING (
       --- INSERTING COMPLETELY NEW ROWS
       SELECT src_null.*, NULL AS mergeKey, 'INSERT' AS action FROM content_job.temp.df_sha256_hashtags src_null
       LEFT JOIN content_job.silver.hashtags tgt ON src_null.hashtag_id = tgt.hashtag_id AND tgt.is_current = true
       WHERE tgt.hashtag_id IS NULL OR tgt.sha_key <> src_null.sha_key
       
       UNION ALL

       --- ROWS THAT HAD CHANGED
       SELECT src.*, src.hashtag_id AS mergeKey, 'UPDATE' AS action FROM content_job.temp.df_sha256_hashtags src
       JOIN content_job.silver.hashtags tgt ON src.hashtag_id = tgt.hashtag_id 
       WHERE tgt.is_current = true AND tgt.sha_key <> src.sha_key

       UNION ALL
       -- DELETED ROWS
       SELECT tgt.hashtag_id, tgt.tag_text, tgt.first_use_time, tgt.valid_from, tgt.sha_key, tgt.hashtag_id AS mergeKey, 'DELETE' AS action
       FROM content_job.silver.hashtags tgt
       LEFT JOIN content_job.temp.df_sha256_hashtags src ON tgt.hashtag_id = src.hashtag_id 
       WHERE src.hashtag_id IS NULL AND tgt.is_current = true

       ) src
ON tgt.hashtag_id = src.mergeKey AND tgt.is_current = true

WHEN MATCHED AND tgt.sha_key <> src.sha_key OR src.action = 'DELETE' THEN UPDATE
SET 
    tgt.valid_to = src.ingest_time,
    tgt.is_current = false


WHEN NOT MATCHED AND src.action = 'INSERT' THEN
INSERT 
    (
    hashtag_id,
    tag_text,
    first_use_time,
    sha_key,
    valid_from,
    valid_to,
    is_current
    )
    VALUES
    (
    src.hashtag_id,
    src.tag_text ,
    src.first_use_time,
    src.sha_key,
    src.ingest_time,
    to_timestamp('9999-12-31 23:59:59'),
    true
    )

"""


spark.sql(sql_code)




sql_code = """
CREATE TABLE IF NOT EXISTS content_job.silver.post_hashtag(
    post_id INT,
    hashtag_id INT,
    sha_key STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN 
)

USING DELTA
"""

spark.sql(sql_code)


sql_code ="""
MERGE INTO content_job.silver.post_hashtag tgt
USING (
    -- COMPLETELY NEW ROWS
    SELECT src_null.*, NULL AS mergeKey_1, NULL AS mergeKey_2,'INSERT' AS action
    FROM content_job.temp.df_sha256_post_hashtag src_null 
    LEFT JOIN content_job.silver.post_hashtag tgt 
    ON src_null.post_id = tgt.post_id AND src_null.hashtag_id = tgt.hashtag_id AND tgt.is_current = true
    WHERE tgt.post_id IS NULL AND tgt.hashtag_id IS NULL
    
    UNION ALL 

    -- CHANGED ROWS
    SELECT src.* , src.post_id AS mergeKey_1, src.hashtag_id AS mergeKey_2, 'UPDATE' AS action
    FROM content_job.temp.df_sha256_post_hashtag src 
    JOIN content_job.silver.post_hashtag tgt ON src.post_id = tgt.post_id AND src.hashtag_id = tgt.hashtag_id 
    WHERE src.sha_key <> tgt.sha_key AND tgt.is_current

    -- DELETE ROWS
    UNION ALL

    SELECT tgt.post_id, tgt.hashtag_id, tgt.valid_from, tgt.sha_key, tgt.post_id AS mergeKey_1, tgt.hashtag_id AS mergeKey_2, 'DELETE' AS action
    FROM content_job.silver.post_hashtag tgt 
    LEFT JOIN content_job.temp.df_sha256_post_hashtag src 
    ON src.post_id = tgt.post_id AND src.hashtag_id = tgt.hashtag_id 
    WHERE src.post_id IS NULL AND src.hashtag_id IS NULL AND tgt.is_current


    ) src
ON tgt.post_id = src.mergeKey_1 AND tgt.hashtag_id = src.mergeKey_2 AND tgt.is_current = true 

WHEN MATCHED AND tgt.sha_key <> src.sha_key OR src.action = 'DELETE' THEN UPDATE 
SET 
    tgt.valid_to = src.ingest_time,
    tgt.is_current = false

WHEN NOT MATCHED THEN 
INSERT(
       post_id,
       hashtag_id,
       sha_key,
       valid_from,
       valid_to,
       is_current
       )
VALUES(
       src.post_id,
       src.hashtag_id,
       src.sha_key,
       src.ingest_time,
       to_timestamp('9999-12-31 23:59:59'),
       true
       )


"""

spark.sql(sql_code)


sql_code = """
CREATE TABLE IF NOT EXISTS content_job.silver.comments(
    comment_id INT,
    author_account_id INT,
    post_id INT,
    created_at_time INT,
    comment_text STRING,
    status VARCHAR(50),
    is_image BOOLEAN,
    sha_key STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
    
)
    
USING DELTA;

"""

spark.sql(sql_code)



sql_code = """
MERGE INTO content_job.silver.comments tgt
USING (
        --- INSERT NEW ROWS
        SELECT src_null.*, NULL AS mergeKey, 'INSERT' AS action FROM content_job.temp.df_sha256_comments src_null
        LEFT JOIN content_job.silver.comments tgt ON src_null.comment_id = tgt.comment_id 
        WHERE tgt.comment_id IS NULL 

        UNION ALL

        -- ROWS THAT CHANGED IN THE PAST
        SELECT src.*, src.comment_id AS mergeKey, 'UPDATE' AS action FROM content_job.temp.df_sha256_comments src
        JOIN content_job.silver.comments tgt ON src.comment_id = tgt.comment_id
        WHERE tgt.is_current = true AND src.sha_key <> tgt.sha_key


       ) src
ON tgt.comment_id = src.mergeKey AND tgt.is_current = true

WHEN MATCHED AND tgt.sha_key <> src.sha_key THEN UPDATE 
SET 
    tgt.valid_to = src.ingest_time,
    tgt.is_current = false

WHEN NOT MATCHED THEN 
INSERT(
      comment_id,
      author_account_id,
      post_id,
      created_at_time,
      comment_text,
      status,
      is_image,
      sha_key,
      valid_from,
      valid_to,
      is_current
      ) 
VALUES(
      src.comment_id,
      src.author_account_id,
      src.post_id,
      src.created_at_time,
      src.comment_text,
      src.status,
      src.is_image,
      src.sha_key,
      src.ingest_time,
      to_timestamp('9999-12-31 23:59:59'),
      true
      )

"""

spark.sql(sql_code)


