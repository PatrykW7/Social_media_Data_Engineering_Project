CREATE OR REFRESH MATERIALIZED VIEW content.target.gold_top_account_growth_per_year AS 
SELECT cte.year, cte.user, cte.number_of_followers FROM (
      SELECT DISTINCT t.year,  f.followed_account_id user, COUNT(f.follower_account_id) number_of_followers, row_number() OVER (PARTITION BY t.year ORDER BY COUNT(f.follower_account_id) DESC) row_number
      FROM target.silver_follow f
      INNER JOIN target.time t ON f.followed_at_time_id = t.time_id
      WHERE f.status = "active" 
      GROUP BY t.year, f.followed_account_id
) cte
WHERE row_number = 1

;

CREATE OR REFRESH MATERIALIZED VIEW content.target.gold_advertiser_quarterly_spend AS 
SELECT CONCAT(t.year, '-', t.quarter) year_quart, adv.advertiser_name, COALESCE(SUM(ad.price_USD),0) USD_price, COALESCE(SUM(ad.Euro_price),0) EUR_price FROM target.silver_advertisers adv
LEFT JOIN target.silver_advertisements ad ON adv.advertiser_id = ad.advertiser_id
LEFT JOIN target.time t ON t.time_id = ad.created_at_time
GROUP BY t.year, t.quarter, adv.advertiser_name
ORDER BY 1 ASC, 3 DESC

;

CREATE OR REFRESH MATERIALIZED VIEW content.target.gold_post_popularity_score AS 
SELECT t.year, t.month, p.post_id, p.author_id, p.visibility, p.language_code, r.reaction_type, COUNT(r.reaction_id) num_reacts, COUNT(c.comment_id) num_comments, COUNT(h.hashtag_id) num_hashtags FROM target.silver_posts p
LEFT JOIN target.silver_comments c ON p.post_id = c.post_id
LEFT JOIN target.silver_reactions r ON p.post_id = r.post_id
LEFT JOIN target.time t ON p.created_at_time = t.time_id
LEFT JOIN target.silver_post_hashtags h ON p.post_id = h.post_id
WHERE is_deleted = False
GROUP BY t.year, t.month, p.post_id, p.author_id, p.visibility, p.language_code, r.reaction_type
ORDER BY 1, 2 

;



CREATE OR REFRESH MATERIALIZED VIEW content.target.gold_user_top_hashtags AS 
SELECT DISTINCT a.account_id, (
  SELECT LISTAGG(cte.tag_text, ', ') FROM (
    SELECT DISTINCT a2.account_id, h2.tag_text, COUNT(ph2.post_id), DENSE_RANK() OVER (PARTITION BY a2.account_id ORDER BY COUNT(ph2.post_id) DESC) row_number FROM target.silver_account_user a2
    INNER JOIN target.silver_posts p2 ON a2.account_id = p2.author_id
    INNER JOIN target.silver_post_hashtags ph2 ON p2.post_id = ph2.post_id
    INNER JOIN target.silver_hashtags h2 ON ph2.hashtag_id = h2.hashtag_id
    WHERE a.account_id = a2.account_id
    GROUP BY a2.account_id, h2.tag_text
    ORDER BY 1, 3 DESC) cte
  WHERE cte.row_number IN (1,2,3)
  GROUP BY cte.account_id
) hashtags FROM target.silver_account_user a
INNER JOIN target.silver_posts p ON a.account_id = p.author_id
INNER JOIN target.silver_post_hashtags ph ON p.post_id = ph.post_id
INNER JOIN target.silver_hashtags h ON ph.hashtag_id = h.hashtag_id 
ORDER BY 1


;

CREATE OR REFRESH MATERIALIZED VIEW content.target.gold_account_master_view AS 
SELECT acc.account_id, acc.account_name, acc.is_group, det.accountAgeCategory, det.profileCompletenessScore, det.friendsCount, det.isVerified,
det.verificationConfidence, det.potentialInfluencer FROM content.target.silver_account_user acc 
LEFT JOIN content.target.silver_account_user_details det ON acc.account_id = det.userId

;




















