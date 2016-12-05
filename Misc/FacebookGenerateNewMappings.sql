/******
Description: Script to generate campaign group names that needs to be
mapped by Manoj (or his team) before proceeding with building the final
table that has last 60 days facebook impression data.

Author: Phyo Thiha
Last Modified: Nov 14, 2016
******/

SELECT
    now();
DROP TABLE
    IF EXISTS impressions_lj_campaign_gp_mappings;
DROP TABLE
    IF EXISTS unmapped_campaign_gp_names;

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
