/*
CREATE OR REFRESH MATERIALIZED VIEW content.target.gold_top_account_growth_per_year AS 
SELECT cte.year, cte.user, cte.number_of_followers FROM (
      SELECT DISTINCT t.year,  f.followed_account_id user, COUNT(f.follower_account_id) number_of_followers, row_number() OVER (PARTITION BY t.year ORDER BY COUNT(f.follower_account_id) DESC) row_number
      FROM target.silver_follow f
      INNER JOIN target.time t ON f.followed_at_time_id = t.time_id
      WHERE f.status = "active" 
      GROUP BY t.year, f.followed_account_id
) cte
WHERE row_number = 1

*/