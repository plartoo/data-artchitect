/******
Description: Script to generate the last 60 days Facebook impression and
spend data allocated using population weight. That weighted data is
unfolded into ziplevel using dma_to_zip mapping.
Author: Phyo Thiha
Last Modified: Dec 6, 2016
Note: As of Dec 6, 2016, this script takes about 1.5 mins to complete.
******/
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_facebook_last_60_days;
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_temp_fb_mapped_by_campaign_gp_names_and_aggregated;
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_temp_dma_to_zip_allocated_and_population_weight_applied;

CREATE TEMP TABLE gaintheory_us_targetusa_14.incampaign_temp_fb_mapped_by_campaign_gp_names_and_aggregated ON COMMIT PRESERVE
ROWS AS
(
    SELECT
        b.fb_mapped_campaign_group_name AS fb_mapped_campaign_group_name,
        a.fb_dma_code,
        a.fb_dma_name,
        a.fb_date,
        SUM(a.fb_total_impressions) AS fb_total_impressions,
        --        SUM(a.fb_mobile_impressions)          AS fb_mobile_impressions,
        --        SUM(a.fb_desktop_impressions)         AS fb_desktop_impressions,
        --        SUM(a.fb_newsfeed_impressions)        AS fb_newsfeed_impressions,
        --        SUM(a.fb_rhs_impressions)             AS fb_rhs_impressions,
        --        SUM(a.fb_instagram_impressions)       AS fb_instagram_impressions,
        --        SUM(a.fb_audiencenetwork_impressions) AS fb_audiencenetwork_impressions,
        SUM(a.fb_total_spend) AS fb_total_spend
        --        ,
        --        SUM(a.fb_mobile_spend)                AS fb_mobile_spend,
        --        SUM(a.fb_desktop_spend)               AS fb_desktop_spend,
        --        SUM(a.fb_newsfeed_spend)              AS fb_newsfeed_spend,
        --        SUM(a.fb_rhs_spend)                   AS fb_rhs_spend,
        --        SUM(a.fb_instagram_spend)             AS fb_instagram_spend,
        --        SUM(a.fb_audiencenetwork_spend)       AS fb_audiencenetwork_spend
    FROM
        gaintheory_us_targetusa_14.v_incampaign_facebook_impressions_and_spend AS a
    LEFT JOIN
        gaintheory_us_targetusa_14.incampaign_facebook_mapped_campaign_group_name AS b
    ON
        a.fb_campaign_name = b.fb_campaign_group_name
    WHERE
        a.fb_date >= (GETDATE()-60)::DATE
    AND b.fb_mapped_campaign_group_name IS NOT NULL
    GROUP BY
        fb_mapped_campaign_group_name,
        fb_dma_code,
        fb_dma_name,
        fb_date );
INSERT
INTO
    gaintheory_us_targetusa_14.incampaign_facebook_log VALUES
    (
        'Facebook Extract: STEP 3',
        NOW(),
        'Aggregated Facebook data that is already mapped'
    );

CREATE TEMP TABLE gaintheory_us_targetusa_14.incampaign_temp_dma_to_zip_allocated_and_population_weight_applied ON COMMIT
PRESERVE ROWS AS
(
    SELECT
        a.fb_mapped_campaign_group_name,
        a.fb_dma_code,
        b.incampaign_zipcode AS fb_zipcode,
        a.fb_dma_name,
        a.fb_date,
        b.incampaign_wt_population,
        --        (b.incampaign_wt_population * a.fb_mobile_impressions)          AS
        -- fb_mobile_impressions,
        --        (b.incampaign_wt_population * a.fb_desktop_impressions)         AS
        -- fb_desktop_impressions,
        --        (b.incampaign_wt_population * a.fb_newsfeed_impressions)        AS
        -- fb_newsfeed_impressions,
        --        (b.incampaign_wt_population * a.fb_rhs_impressions)             AS
        -- fb_rhs_impressions,
        --        (b.incampaign_wt_population * a.fb_instagram_impressions)       AS
        -- fb_instagram_impressions,
        --        (b.incampaign_wt_population * a.fb_audiencenetwork_impressions) AS
        --
        -- fb_audiencenetwork_impressions,
        (b.incampaign_wt_population * a.fb_total_impressions) AS fb_total_impressions,
        --        (b.incampaign_wt_population * a.fb_mobile_spend)          AS fb_mobile_spend,
        --        (b.incampaign_wt_population * a.fb_desktop_spend)         AS fb_desktop_spend,
        --        (b.incampaign_wt_population * a.fb_newsfeed_spend)        AS fb_newsfeed_spend,
        --        (b.incampaign_wt_population * a.fb_rhs_spend)             AS fb_rhs_spend,
        --        (b.incampaign_wt_population * a.fb_instagram_spend)       AS fb_instagram_spend,
        --        (b.incampaign_wt_population * a.fb_audiencenetwork_spend) AS
        -- fb_audiencenetwork_spend,
        (b.incampaign_wt_population * a.fb_total_spend) AS fb_total_spend
    FROM
        gaintheory_us_targetusa_14.incampaign_temp_fb_mapped_by_campaign_gp_names_and_aggregated AS a
    LEFT JOIN
        gaintheory_us_targetusa_14.incampaign_dma_to_zipcode_and_population_weight AS b
    ON
        a.fb_dma_code = b.incampaign_dmac
    WHERE
        b.incampaign_dmac IS NOT NULL ); --  approx. 23 million rows here
INSERT
INTO
    gaintheory_us_targetusa_14.incampaign_facebook_log VALUES
    (
        'Facebook Extract: STEP 4',
        NOW(),
        'Applied population weight and DMC to Zip allocation'
    );

CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_facebook_last_60_days AS
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
            gaintheory_us_targetusa_14.incampaign_temp_dma_to_zip_allocated_and_population_weight_applied
        GROUP BY
            Geography,
            Product,
            Campaign,
            VariableName,
            Outlet,
            Creative,
            Period
        UNION ALL
        SELECT
            'Geo_'||fb_zipcode            AS Geography,
            'Target'                      AS Product,
            fb_mapped_campaign_group_name AS Campaign,
            'Total_FB_Spend'              AS VariableName,
            'Total'                       AS Outlet,
            'Total'                       AS Creative,
            fb_date::DATE                 AS Period,
            SUM(fb_total_spend)           AS VariableValue
        FROM
            gaintheory_us_targetusa_14.incampaign_temp_dma_to_zip_allocated_and_population_weight_applied
        GROUP BY
            Geography,
            Product,
            Campaign,
            VariableName,
            Outlet,
            Creative,
            Period
    );
--  approx. 43 million rows here
INSERT
INTO
    gaintheory_us_targetusa_14.incampaign_facebook_log VALUES
    (
        'Facebook Extract: STEP 5',
        NOW(),
        'Created IRT table'
    );
