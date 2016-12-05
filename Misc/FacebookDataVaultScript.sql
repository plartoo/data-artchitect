/******
Description: Script to generate the last 60 days Facebook impression
data allocated using population weight and unfolded into ziplevel using
dma_to_zip mapping.

Author: Phyo Thiha
Last Modified: Nov 10, 2016
******/

SELECT
    now();
DROP TABLE
    IF EXISTS impressions_lj_campaign_gp_mappings;
DROP TABLE
    IF EXISTS unmapped_campaign_gp_names;
DROP TABLE
    IF EXISTS impressions_mapped_by_campaign_gp_names_and_aggregated;
DROP TABLE
    IF EXISTS dma_to_zip_allocated_and_population_weight_applied;
DROP TABLE
    IF EXISTS transformed_incampaign_facebook_impressions;
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_facebook_last_60_days;

CREATE TEMP TABLE impressions_lj_campaign_gp_mappings ON COMMIT PRESERVE ROWS AS
(
    SELECT
        a.fb_campaign_group_name,
        b.fb_mapped_campaign_group_name,
        a.fb_dma_code,
        a.fb_dma_name,
        a.fb_date,
        a.fb_total_impressions,
        a.fb_mobile_impressions,
        a.fb_desktop_impressions,
        a.fb_newsfeed_impressions,
        a.fb_rhs_impressions,
        a.fb_instagram_impressions,
        a.fb_audiencenetwork_impressions
    FROM
        incampaign_facebook_impressions_zipcode AS a
    LEFT JOIN
        incampaign_facebook_mapped_campaign_group_name AS b
    ON
        a.fb_campaign_group_name = b.fb_campaign_group_name
    WHERE
        a.fb_date >= (GETDATE()-60)::DATE ); 
-- approx. 14 million rows of which ~ 500,000 rows are unmatched and 13.5 million rows are matched

CREATE TEMP TABLE unmapped_campaign_gp_names ON COMMIT PRESERVE ROWS AS
(
    SELECT DISTINCT
        fb_campaign_group_name
    FROM
        impressions_lj_campaign_gp_mappings AS a
    WHERE
        fb_mapped_campaign_group_name IS NULL );
-- approx. < 1,000 rows

CREATE TABLE
    IF NOT EXISTS gaintheory_us_targetusa_14.incampaign_facebook_campaign_group_names_to_map
    (
        fb_campaign_group_name VARCHAR(1000)
    );
MERGE
INTO
    gaintheory_us_targetusa_14.incampaign_facebook_campaign_group_names_to_map AS t
USING
    unmapped_campaign_gp_names AS s
ON
    t.fb_campaign_group_name = s.fb_campaign_group_name
WHEN MATCHED
    THEN
UPDATE
SET
    fb_campaign_group_name = s.fb_campaign_group_name
WHEN NOT MATCHED
    THEN
INSERT
    (
        fb_campaign_group_name
    )
    VALUES
    (
        s.fb_campaign_group_name
    );




CREATE TEMP TABLE impressions_mapped_by_campaign_gp_names_and_aggregated ON COMMIT PRESERVE ROWS AS
(
    SELECT
        a.fb_mapped_campaign_group_name,
        a.fb_dma_code,
        a.fb_dma_name,
        a.fb_date,
        SUM(a.fb_total_impressions)           AS fb_total_impressions,
        SUM(a.fb_mobile_impressions)          AS fb_mobile_impressions,
        SUM(a.fb_desktop_impressions)         AS fb_desktop_impressions,
        SUM(a.fb_newsfeed_impressions)        AS fb_newsfeed_impressions,
        SUM(a.fb_rhs_impressions)             AS fb_rhs_impressions,
        SUM(a.fb_instagram_impressions)       AS fb_instagram_impressions,
        SUM(a.fb_audiencenetwork_impressions) AS fb_audiencenetwork_impressions
    FROM
        impressions_lj_campaign_gp_mappings AS a
    WHERE
        fb_mapped_campaign_group_name IS NOT NULL
    GROUP BY
        fb_mapped_campaign_group_name,
        fb_dma_code,
        fb_dma_name,
        fb_date );
-- approx. 120,000 rows here

CREATE TEMP TABLE dma_to_zip_allocated_and_population_weight_applied ON COMMIT PRESERVE ROWS AS
(
    SELECT
        a.fb_mapped_campaign_group_name,
        a.fb_dma_code,
        b.incampaign_zipcode AS fb_zipcode,
        a.fb_dma_name,
        a.fb_date,
        b.incampaign_wt_population,
        (b.incampaign_wt_population * a.fb_mobile_impressions)          AS fb_mobile_impressions,
        (b.incampaign_wt_population * a.fb_desktop_impressions)         AS fb_desktop_impressions,
        (b.incampaign_wt_population * a.fb_newsfeed_impressions)        AS fb_newsfeed_impressions,
        (b.incampaign_wt_population * a.fb_rhs_impressions)             AS fb_rhs_impressions,
        (b.incampaign_wt_population * a.fb_instagram_impressions)       AS fb_instagram_impressions,
        (b.incampaign_wt_population * a.fb_audiencenetwork_impressions) AS
                                                                 fb_audiencenetwork_impressions,
        (b.incampaign_wt_population * a.fb_total_impressions) AS fb_total_impressions
    FROM
        impressions_mapped_by_campaign_gp_names_and_aggregated AS a
    LEFT JOIN
        incampaign_dma_to_zipcode_and_population_weight AS b
    ON
        a.fb_dma_code = b.incampaign_dmac
    WHERE
        b.incampaign_dmac IS NOT NULL );
--  approx. 23 million rows here

CREATE TEMP TABLE transformed_incampaign_facebook_impressions ON COMMIT PRESERVE ROWS AS
(
    SELECT
        'Geo_'||fb_zipcode            AS Geography,
        'Target'                      AS Product,
        fb_mapped_campaign_group_name AS Campaign,
        'Total_FB_Imp'                AS VariableName,
        'Total'                       AS Outlet,
        'Total'                       AS Creative,
        fb_date::DATE                 AS Period,
        SUM(fb_total_impressions)     AS VariableValue
    FROM
        dma_to_zip_allocated_and_population_weight_applied
    GROUP BY
        Geography,
        Product,
        Campaign,
        VariableName,
        Outlet,
        Creative,
        Period );
--  approx. 23 million rows here

CREATE TABLE
    incampaign_facebook_last_60_days AS
    (
        SELECT
            *
        FROM
            transformed_incampaign_facebook_impressions
    );
SELECT
    now(); -- as of Nov 10, 2016, the entire script runs under 2 minutes
