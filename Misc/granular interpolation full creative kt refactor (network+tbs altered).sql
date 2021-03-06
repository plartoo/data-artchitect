/* All the Rentrak data for the past month with reference data applied */
DROP VIEW IF EXISTS rt_prev_month_network_zip_mk;
CREATE VIEW rt_prev_month_network_zip_mk AS
SELECT rentrak_network as network, rentrak_zip as zip, SUM(rentrak_ad_zip_aa) as impressions
FROM incampaign_rentrak_zipcode a
INNER JOIN (
        SELECT rentrak_spot_id, rentrak_week, rentrak_network
        FROM incampaign_rentrak_spotid
        WHERE rentrak_ad_time::date >=
        (
                SELECT ADD_MONTHS(MAX(rentrak_ad_time::date-1), -1)
                FROM incampaign_rentrak_spotid
        )
) b
ON a.rentrak_spot_id = b.rentrak_spot_id AND a.rentrak_week = b.rentrak_week
GROUP BY rentrak_network, rentrak_zip;

/* The global zip level multiplier for where a network doesn't exist in the current RT dataset */
DROP VIEW IF EXISTS rt_global_factors_mk;
CREATE VIEW rt_global_factors_mk AS
SELECT  1 as global_match,
        zip AS Geography,
        impressions/(
                SELECT SUM(impressions)
                FROM rt_prev_month_network_zip_mk
        ) as factor
FROM(
        SELECT  zip, SUM(impressions) as impressions
        FROM rt_prev_month_network_zip_mk
        GROUP BY zip
) a;

/* KeepingTrac data with their equivalent Rentrak network where matches exist
   Commented code restricts the dataset to only those days not available in the current Rentrak datset*/
DROP VIEW IF EXISTS kt_with_rt_network_mk;
CREATE VIEW kt_with_rt_network_mk AS
SELECT  CASE 
                WHEN Air_Time < '05:00:00' THEN Air_Date + 1
                ELSE Air_date
        END AS Air_Date,
        Air_Time,
        network AS kt_network,
        rt_network,
        Air_ISCI AS kt_creative_id,
        kt_creative_clean AS kt_creative,
        Spot_Length,
        Act_Impression
FROM keepingtrac kt 
LEFT JOIN js_rt_kt_reference rf
ON kt.network = rf.kt_network
LEFT JOIN kt_creative_cleaned cr
ON kt.Air_ISCI = cr.kt_creative_id
WHERE Air_Date IS NOT NULL 
AND Type_of_Demographic = 2
AND NOT Media_Type = 'Syndication';

/* The combined KeepingTrac and Rentrak data where network appears in both datasets,
   else a row containing a null VariableValue exists
   Also applies the naming convention matching current modelling data */
DROP VIEW IF EXISTS rt_kt_combined_raw_mk;
CREATE VIEW rt_kt_combined_raw_mk AS
SELECT  zip AS Geography,
        'Target' AS Product,
        kt.rt_network AS Campaign,
        'TV_Imp' AS VariableName,
        CAST(Spot_Length AS VARCHAR) AS Outlet,
        kt_creative AS Creative,
        Air_Date AS Period,
        Act_Impression*factor as VariableValue,
        Act_Impression
FROM kt_with_rt_network_mk kt
LEFT JOIN (
        SELECT a.network AS rt_network, zip, impressions/total_impressions AS factor
        FROM rt_prev_month_network_zip_mk a
        LEFT JOIN (
                SELECT network, SUM(impressions) AS total_impressions
                FROM rt_prev_month_network_zip_mk
                GROUP BY network
        ) b
        ON a.network = b.network
) rt
ON kt.rt_network = rt.rt_network;


DROP VIEW IF EXISTS global_kt_lift_mk;
CREATE VIEW global_kt_lift_mk AS
SELECT 'Target' AS Product, rt_impressions/kt_impressions AS factor
FROM (
SELECT 1 AS global_join, SUM(impressions) AS rt_impressions
FROM rt_prev_month_network_zip_mk
) a
JOIN (
SELECT 1 AS global_join, SUM(Act_Impression) as kt_impressions
FROM kt_with_rt_network_mk
WHERE Air_Date BETWEEN
        (
                SELECT ADD_MONTHS(MAX(rentrak_ad_time::date-1), -1)
                FROM incampaign_rentrak_spotid
        ) AND (
                SELECT MAX(rentrak_ad_time::date-1)
                FROM incampaign_rentrak_spotid
        )
) b
ON a.global_join = b.global_join;


DROP VIEW IF EXISTS network_kt_lift_mk;
CREATE VIEW network_kt_lift_mk AS
SELECT 'Target' AS Product, network, rt_impressions/kt_impressions AS factor
FROM (
SELECT network, SUM(impressions) AS rt_impressions
FROM rt_prev_month_network_zip_mk
GROUP BY network
) a
INNER JOIN (
SELECT rt_network, SUM(Act_Impression) as kt_impressions
FROM kt_with_rt_network_mk
WHERE Air_Date BETWEEN
        (
                SELECT ADD_MONTHS(MAX(rentrak_ad_time::date-1), -1)
                FROM incampaign_rentrak_spotid
        ) AND (
                SELECT MAX(rentrak_ad_time::date-1)
                FROM incampaign_rentrak_spotid
        )
GROUP BY rt_network
) b
ON a.network = b.rt_network;


/* Code providing standard modelling data from Rentrak dataset */
DROP TABLE IF EXISTS rentrak_granular_clean_creative_nw_refactorkt_tbs;
CREATE TABLE rentrak_granular_clean_creative_nw_refactorkt_tbs AS
SELECT  a.rentrak_zip AS Geography,
        'Target' AS Product,
        b.rentrak_network AS Campaign,
        'TV_Imp' AS VariableName,
        CAST(b.rentrak_runtime_seconds AS VARCHAR) AS Outlet, 
        CASE 
                WHEN c.kt_creative IS NOT NULL THEN c.kt_creative 
                WHEN b.rentrak_ad_copy IS NULL THEN 'unknown'
                ELSE 
                case when rentrak_ad_no in ('1837228','858743') then 'unknown' else b.rentrak_ad_copy end
        END AS Creative,
        b.rentrak_ad_time::date AS Period,
        SUM(a.rentrak_ad_zip_aa) AS VariableValue
FROM gaintheory_us_targetusa_14.incampaign_rentrak_zipcode a 
LEFT JOIN gaintheory_us_targetusa_14.incampaign_rentrak_spotid b
ON a.rentrak_spot_id = b.rentrak_spot_id and a.rentrak_week = b.rentrak_week
LEFT JOIN gaintheory_us_targetusa_14.js_creative_match_deduped_20161016_20161112 c
ON b.rentrak_ad_no = c.rt_creative_id
WHERE b.rentrak_ad_time::date BETWEEN '2016-10-09' AND (
        SELECT MAX(rentrak_ad_time::date-1) - 1
        FROM gaintheory_us_targetusa_14.incampaign_rentrak_spotid
)
GROUP BY Geography, Product, Campaign, VariableName, Outlet, Creative, Period
UNION
SELECT Geography, a.Product, Campaign, VariableName, Outlet, Creative, Period, SUM(VariableValue*factor) AS VariableValue
FROM rt_kt_combined_raw_mk a
LEFT JOIN network_kt_lift_mk b
ON a.Campaign = b.network
WHERE VariableValue IS NOT NULL AND Campaign IS NOT NULL
AND Period BETWEEN (
        SELECT MAX(rentrak_ad_time::date-1)
        FROM gaintheory_us_targetusa_14.incampaign_rentrak_spotid
) AND '2016-11-19'
AND Campaign != 'TBS'
GROUP BY Geography, a.Product, Campaign, VariableName, Outlet, Creative, Period
UNION
SELECT Geography, a.Product, Campaign, VariableName, Outlet, Creative, Period, SUM(Act_Impression*b.factor*c.factor) as VariableValue
FROM (
        SELECT 1 as global_match, Product, Campaign, VariableName, Outlet, Creative, Period, Act_Impression
        FROM rt_kt_combined_raw_mk
        WHERE VariableValue IS NULL AND Campaign IS NOT NULL
        AND Period BETWEEN (
                SELECT MAX(rentrak_ad_time::date-1)
                FROM gaintheory_us_targetusa_14.incampaign_rentrak_spotid
        ) AND '2016-11-19'
) a
LEFT JOIN rt_global_factors_mk b
ON a.global_match = b.global_match
LEFT JOIN global_kt_lift_mk c
ON a.Product = c.Product
GROUP BY Geography, a.Product, Campaign, VariableName, Outlet, Creative, Period
UNION
SELECT Geography, a.Product, Campaign, VariableName, Outlet, Creative, Period, SUM(VariableValue*factor) AS VariableValue
FROM rt_kt_combined_raw_mk a
LEFT JOIN global_kt_lift_mk b
ON a.Product = b.Product
WHERE VariableValue IS NOT NULL AND Campaign IS NOT NULL
AND Period BETWEEN (
        SELECT MAX(rentrak_ad_time::date-1)
        FROM gaintheory_us_targetusa_14.incampaign_rentrak_spotid
) AND '2016-11-19'
AND Campaign = 'TBS'
GROUP BY Geography, a.Product, Campaign, VariableName, Outlet, Creative, Period
ORDER BY Geography, Product, Campaign, VariableName, Outlet, Creative, Period;



SELECT Period, SUM(VariableValue) AS VariableValue
FROM rentrak_granular_clean_creative_refactorkt
GROUP BY Period
ORDER BY Period;

SELECT Period, SUM(VariableValue) AS VariableValue
FROM rentrak_granular_clean_creative_nw_refactorkt_tbs
GROUP BY Period
ORDER BY Period;

