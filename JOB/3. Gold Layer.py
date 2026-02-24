sql_code = """
CREATE OR REPLACE TABLE content_job.gold.account_master_view 
AS 
    SELECT acc.account_id, 
           acc.account_name, 
           acc.is_group, 
           det.accountAgeCategory, 
           det.profileCompletenessScore, 
           det.friendsCount, 
           det.isVerified,
           det.verificationConfidence, 
           det.potentialInfluencer, 
           det.potentialBot 
FROM content_job.silver.account_user acc 
LEFT JOIN content_job.silver.account_details det ON acc.account_id = det.userId
WHERE acc.is_current = True
AND det.is_current = True
"""

spark.sql(sql_code)