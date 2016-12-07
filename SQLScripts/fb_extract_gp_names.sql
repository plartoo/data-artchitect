/******
Description: Script to extract the unmapped campaign_group_names
based on incampaign_facebook_mapped_campaign_group_name
and incampaign_facebook_impressions_and_spend.

Author: Phyo Thiha
Last Modified: Dec 6, 2016
******/
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_temp_impressions_and_spend_lj_campaign_gp_mappings;
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_temp_unmapped_campaign_gp_names;

CREATE TEMP TABLE
    gaintheory_us_targetusa_14.incampaign_temp_impressions_and_spend_lj_campaign_gp_mappings ON COMMIT PRESERVE ROWS AS
    (
        SELECT
            a.fb_campaign_group_name,
            b.fb_mapped_campaign_group_name
        FROM
            gaintheory_us_targetusa_14.incampaign_facebook_impressions_and_spend AS a
        LEFT JOIN
            gaintheory_us_targetusa_14.incampaign_facebook_mapped_campaign_group_name AS b
        ON
            a.fb_campaign_group_name = b.fb_campaign_group_name
        WHERE
            a.fb_date >= (GETDATE()-60)::DATE
    );

CREATE TEMP TABLE gaintheory_us_targetusa_14.incampaign_temp_unmapped_campaign_gp_names ON COMMIT PRESERVE ROWS AS
(
    SELECT DISTINCT
        fb_campaign_group_name
    FROM
        gaintheory_us_targetusa_14.incampaign_temp_impressions_and_spend_lj_campaign_gp_mappings AS a
    WHERE
        fb_mapped_campaign_group_name IS NULL );

CREATE TABLE
    IF NOT EXISTS gaintheory_us_targetusa_14.incampaign_facebook_campaign_group_names_to_map
    (
        fb_campaign_group_name VARCHAR(1000)
    );
INSERT
INTO
    gaintheory_us_targetusa_14.incampaign_facebook_log VALUES
    (
        'Facebook Extract: STEP 1',
        NOW(),
        'Extracted unmapped/new campaign group names'
    );


MERGE
INTO
    gaintheory_us_targetusa_14.incampaign_facebook_campaign_group_names_to_map AS t
USING
    gaintheory_us_targetusa_14.incampaign_temp_unmapped_campaign_gp_names AS s
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
INSERT
INTO
    gaintheory_us_targetusa_14.incampaign_facebook_log VALUES
    (
        'Facebook Extract: STEP 2',
        NOW(),
        'Merged unmapped/new campaign group names'
    );
COMMIT;