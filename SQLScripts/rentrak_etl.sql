/* Note: Start and End date is determined as such:
There is usually up to 9 days differece when we get RenTrak data (e.g., Dec 19, 2016)
and the latest KT data (which is only up to, say, Dec 10, 2016). So the end date must 
be Dec 10, 2016 and start date is (Dec 10 - 61 days) = Oct 10, 2016.
*/

/* All the Rentrak data for the past month with reference data applied */
DROP TABLE IF EXISTS incampaign_tmp_rt_prev_month_network_zip;
CREATE TABLE incampaign_tmp_rt_prev_month_network_zip AS
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
DROP TABLE IF EXISTS incampaign_tmp_rt_global_factors;
CREATE TABLE incampaign_tmp_rt_global_factors AS
SELECT  1 as global_match,
        zip AS Geography,
        impressions/(
                SELECT SUM(impressions)
                FROM incampaign_tmp_rt_prev_month_network_zip
        ) as factor
FROM(
        SELECT  zip, SUM(impressions) as impressions
        FROM incampaign_tmp_rt_prev_month_network_zip
        GROUP BY zip
) a;

/* The combined KeepingTrac and Rentrak data where network appears in both datasets,
   else a row containing a null VariableValue exists
   Also applies the naming convention matching current modelling data */
DROP TABLE IF EXISTS incampaign_tmp_rt_kt_combined_raw;
CREATE TABLE incampaign_tmp_rt_kt_combined_raw AS
SELECT  zip AS Geography,
        'Target' AS Product,
        kt.rt_network AS Campaign,
        'TV_Imp' AS VariableName,
        CAST(Spot_Length AS VARCHAR) AS Outlet,
        kt_creative AS Creative,
        Air_Date AS Period,
        Act_Impression*factor as VariableValue,
        Act_Impression
FROM incampaign_tmp_kt_with_rt_network kt
LEFT JOIN (
        SELECT a.network AS rt_network, zip, impressions/total_impressions AS factor
        FROM incampaign_tmp_rt_prev_month_network_zip a
        LEFT JOIN (
                SELECT network, SUM(impressions) AS total_impressions
                FROM incampaign_tmp_rt_prev_month_network_zip
                GROUP BY network
        ) b
        ON a.network = b.network
) rt
ON kt.rt_network = rt.rt_network;


DROP TABLE IF EXISTS incampaign_tmp_global_kt_lift;
CREATE TABLE incampaign_tmp_global_kt_lift AS
SELECT 'Target' AS Product, rt_impressions/kt_impressions AS factor
FROM (
SELECT 1 AS global_join, SUM(impressions) AS rt_impressions
FROM incampaign_tmp_rt_prev_month_network_zip
) a
JOIN (
SELECT 1 AS global_join, SUM(Act_Impression) as kt_impressions
FROM incampaign_tmp_kt_with_rt_network
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


DROP TABLE IF EXISTS incampaign_tmp_network_kt_lift;
CREATE TABLE incampaign_tmp_network_kt_lift AS
SELECT 'Target' AS Product, network, rt_impressions/kt_impressions AS factor
FROM (
SELECT network, SUM(impressions) AS rt_impressions
FROM incampaign_tmp_rt_prev_month_network_zip
GROUP BY network
) a
INNER JOIN (
SELECT rt_network, SUM(Act_Impression) as kt_impressions
FROM incampaign_tmp_kt_with_rt_network
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
DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_rentrak_last_60_days;
CREATE TABLE gaintheory_us_targetusa_14.incampaign_rentrak_last_60_days AS
(
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
LEFT JOIN gaintheory_us_targetusa_14.incampaign_rentrak_creative_match_deduped c
ON b.rentrak_ad_no = c.rt_creative_id
WHERE b.rentrak_ad_time::date BETWEEN (GETDATE()-70)::DATE AND (--'2016-10-09' AND (
        SELECT MAX(rentrak_ad_time::date-1) - 1
        FROM gaintheory_us_targetusa_14.incampaign_rentrak_spotid
)
GROUP BY Geography, Product, Campaign, VariableName, Outlet, Creative, Period

UNION

SELECT Geography, a.Product, Campaign, VariableName, Outlet, Creative, Period, SUM(VariableValue*factor) AS VariableValue
FROM gaintheory_us_targetusa_14.incampaign_tmp_rt_kt_combined_raw a
LEFT JOIN gaintheory_us_targetusa_14.incampaign_tmp_network_kt_lift b
ON a.Campaign = b.network
WHERE VariableValue IS NOT NULL AND Campaign IS NOT NULL
AND Period BETWEEN (
        SELECT MAX(rentrak_ad_time::date-1)
        FROM gaintheory_us_targetusa_14.incampaign_rentrak_spotid
) AND (GETDATE()-9)::DATE--'2016-11-19'
AND Campaign != 'TBS'
GROUP BY Geography, a.Product, Campaign, VariableName, Outlet, Creative, Period

UNION

SELECT Geography, a.Product, Campaign, VariableName, Outlet, Creative, Period, SUM(Act_Impression*b.factor*c.factor) as VariableValue
FROM (
        SELECT 1 as global_match, Product, Campaign, VariableName, Outlet, Creative, Period, Act_Impression
        FROM gaintheory_us_targetusa_14.incampaign_tmp_rt_kt_combined_raw
        WHERE VariableValue IS NULL AND Campaign IS NOT NULL
        AND Period BETWEEN (
                SELECT MAX(rentrak_ad_time::date-1)
                FROM gaintheory_us_targetusa_14.incampaign_rentrak_spotid
        ) AND (GETDATE()-9)::DATE--'2016-11-19'
) a
LEFT JOIN gaintheory_us_targetusa_14.incampaign_tmp_rt_global_factors b
ON a.global_match = b.global_match
LEFT JOIN gaintheory_us_targetusa_14.incampaign_tmp_global_kt_lift c
ON a.Product = c.Product
GROUP BY Geography, a.Product, Campaign, VariableName, Outlet, Creative, Period
UNION
SELECT Geography, a.Product, Campaign, VariableName, Outlet, Creative, Period, SUM(VariableValue*factor) AS VariableValue
FROM gaintheory_us_targetusa_14.incampaign_tmp_rt_kt_combined_raw a
LEFT JOIN gaintheory_us_targetusa_14.incampaign_tmp_global_kt_lift b
ON a.Product = b.Product
WHERE VariableValue IS NOT NULL AND Campaign IS NOT NULL
AND Period BETWEEN (
        SELECT MAX(rentrak_ad_time::date-1)
        FROM gaintheory_us_targetusa_14.incampaign_rentrak_spotid
) AND (GETDATE()-9)::DATE--'2016-11-19'
AND Campaign = 'TBS'
GROUP BY Geography, a.Product, Campaign, VariableName, Outlet, Creative, Period
ORDER BY Geography, Product, Campaign, VariableName, Outlet, Creative, Period
)
;
