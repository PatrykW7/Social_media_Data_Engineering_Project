spark.sql("""
          CREATE TABLE IF NOT EXISTS content_job.gold.top_account_growth_per_year(
            year INT,
            user INT,
            number_of_followers INT
          )
          USING DELTA
          CLUSTER BY (year);
 """)


spark.sql("""INSERT OVERWRITE content_job.gold.top_account_growth_per_year 
SELECT cte.year, cte.user, cte.number_of_followers FROM (
      SELECT DISTINCT t.year,  f.followed_account_id user, COUNT(f.follower_account_id) number_of_followers, row_number() OVER (PARTITION BY t.year ORDER BY COUNT(f.follower_account_id) DESC) row_number
      FROM content_job.silver.follow_relationship f
      INNER JOIN content_job.silver.time t ON f.followed_at_time_id = t.time_id
      WHERE f.status = "active" 
      GROUP BY t.year, f.followed_account_id
) cte
WHERE row_number = 1
""")

spark.sql("OPTIMIZE content_job.gold.top_account_growth_per_year ")

spark.sql("VACUUM content_job.gold.top_account_growth_per_year RETAIN 720 HOURS")


spark.sql("""
          CREATE TABLE IF NOT EXISTS content_job.gold.advertiser_quarterly_spend(
            year_quart STRING,
            advertiser_name VARCHAR(250),
            USD_price DOUBLE,
            EUR_price DOUBLE
          )
          USING DELTA
          CLUSTER BY (year_quart);
          """)


spark.sql("""INSERT OVERWRITE content_job.gold.advertiser_quarterly_spend 
SELECT CONCAT(t.year, '-', t.quarter) year_quart, adv.advertiser_name, COALESCE(SUM(ad.price_USD),0) USD_price, COALESCE(SUM(ad.Euro_price),0) EUR_price 
FROM content_job.silver.advertisers adv
LEFT JOIN content_job.silver.advertisements ad ON adv.advertiser_id = ad.advertiser_id
LEFT JOIN content_job.silver.time t ON t.time_id = ad.created_at_time
GROUP BY t.year, t.quarter, adv.advertiser_name
ORDER BY 1 ASC, 3 DESC"""
)

spark.sql("OPTIMIZE content_job.gold.advertiser_quarterly_spend")

spark.sql("VACUUM content_job.gold.advertiser_quarterly_spend RETAIN 720 HOURS")


spark.sql("""
          CREATE TABLE IF NOT EXISTS content_job.gold.post_popularity_score(
            year INT,
            month INT,
            post_id INT,
            author_id INT,
            visibility VARCHAR(200),
            language_code VARCHAR(100),
            reaction_type VARCHAR(25),
            num_reacts INT,
            num_comments INT,
            num_hashtags INT
          )
          USING DELTA
          CLUSTER BY (year ,month);
          """)


spark.sql("""INSERT OVERWRITE content_job.gold.post_popularity_score 
SELECT t.year, t.month, p.post_id, p.author_id, p.visibility, p.language_code, r.reaction_type, COUNT(r.reaction_id) num_reacts, COUNT(c.comment_id) num_comments, COUNT(h.hashtag_id) num_hashtags 
FROM content_job.silver.posts p
LEFT JOIN content_job.silver.comments c ON p.post_id = c.post_id
LEFT JOIN content_job.silver.reactions r ON p.post_id = r.post_id
LEFT JOIN content_job.silver.time t ON p.created_at_time = t.time_id
LEFT JOIN content_job.silver.post_hashtag h ON p.post_id = h.post_id
WHERE is_deleted = False
GROUP BY t.year, t.month, p.post_id, p.author_id, p.visibility, p.language_code, r.reaction_type
ORDER BY 1, 2"""
)

spark.sql("OPTIMIZE content_job.gold.post_popularity_score")

spark.sql("VACUUM content_job.gold.post_popularity_score RETAIN 720 HOURS")


spark.sql("""
          CREATE TABLE IF NOT EXISTS content_job.gold.user_top_hashtags(
            account_id INT,
            hashtags STRING
          )
          USING DELTA
          CLUSTER BY (account_id)
          """)


spark.sql("""INSERT OVERWRITE content_job.gold.user_top_hashtags 
SELECT DISTINCT a.account_id, (
  SELECT LISTAGG(cte.tag_text, ', ') FROM (
    SELECT DISTINCT a2.account_id, h2.tag_text, COUNT(ph2.post_id), DENSE_RANK() OVER (PARTITION BY a2.account_id ORDER BY COUNT(ph2.post_id) DESC) row_number 
    FROM content_job.silver.account_user a2
    INNER JOIN content_job.silver.posts p2 ON a2.account_id = p2.author_id
    INNER JOIN content_job.silver.post_hashtag ph2 ON p2.post_id = ph2.post_id
    INNER JOIN content_job.silver.hashtags h2 ON ph2.hashtag_id = h2.hashtag_id
    WHERE a.account_id = a2.account_id
    GROUP BY a2.account_id, h2.tag_text
    ORDER BY 1, 3 DESC) cte
  WHERE cte.row_number IN (1,2,3)
  GROUP BY cte.account_id
) hashtags FROM content_job.silver.account_user a
INNER JOIN content_job.silver.posts p ON a.account_id = p.author_id
INNER JOIN content_job.silver.post_hashtag ph ON p.post_id = ph.post_id
INNER JOIN content_job.silver.hashtags h ON ph.hashtag_id = h.hashtag_id 
ORDER BY 1"""
)

spark.sql("OPTIMIZE content_job.gold.user_top_hashtags")

spark.sql("VACUUM content_job.gold.user_top_hashtags RETAIN 720 HOURS")


spark.sql("""CREATE TABLE IF NOT EXISTS content_job.gold.account_master_view(
              account_id INT,
              account_name STRING,
              is_group BOOLEAN,
              accountAgeCategory STRING,
              profileCompletenessScore DOUBLE,
              friendsCount INT,
              favouritesCount INT,
              influenceScore DOUBLE,
              account_creation_year_month STRING,
              createdYear INT,
              isVerified INT,
              listedCount INT,
              verificationConfidence STRING,
              potentialInfluencer INT,
              potentialBot INT
          )
          USING DELTA
          CLUSTER BY (account_id)
          """)

spark.sql("""INSERT OVERWRITE content_job.gold.account_master_view 
    SELECT acc.account_id, 
           acc.account_name, 
           acc.is_group, 
           det.accountAgeCategory, 
           det.profileCompletenessScore, 
           det.friendsCount, 
           det.favouritesCount,
           det.influenceScore,
           det.account_creation_year_month,
           det.createdYear,
           det.isVerified,
           det.listedCount,
           det.verificationConfidence, 
           det.potentialInfluencer, 
           det.potentialBot 
FROM content_job.silver.account_user acc 
LEFT JOIN content_job.silver.account_details det ON acc.account_id = det.userId
WHERE acc.is_current = True
AND det.is_current = True
"""
)

spark.sql("OPTIMIZE content_job.gold.account_master_view")

spark.sql("VACUUM content_job.gold.account_master_view RETAIN 720 HOURS")

