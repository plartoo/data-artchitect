/*
Description: Script to extract the unmapped campaign_group_names
based on incampaign_facebook_mapped_campaign_group_name
and v_incampaign_facebook_impressions_and_spend.

Author: Phyo Thiha
Last Modified: Feb 1, 2017

NOTE: After FB changed their schema in Jan 2017, we will use 'fb_campaign_name' 
instead of 'fb_campaign_group_name', but the legacy configuration of tables in 
DataVault means we will stick to 'fb_campaign_group_name' for existing tables
such as 'incampaign_facebook_mapped_campaign_group_name'.
*/

DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_impressions_and_spend_lj_campaign_gp_mappings;
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_unmapped_campaign_names;

CREATE TEMP TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_impressions_and_spend_lj_campaign_gp_mappings ON COMMIT PRESERVE ROWS AS
    (
        SELECT
            a.fb_campaign_name,
            b.fb_mapped_campaign_group_name
        FROM
            gaintheory_us_targetusa_14.v_incampaign_facebook_impressions_and_spend AS a
        LEFT JOIN
            gaintheory_us_targetusa_14.incampaign_facebook_mapped_campaign_group_name AS b
        ON

            a.fb_campaign_name = b.fb_campaign_group_name -- see NOTE in the description above
        WHERE
            a.fb_date >= (GETDATE()-60)::DATE
    );

CREATE TEMP TABLE gaintheory_us_targetusa_14.incampaign_tmp_unmapped_campaign_names ON COMMIT PRESERVE ROWS AS
(
    SELECT DISTINCT
        fb_campaign_name
    FROM
        gaintheory_us_targetusa_14.incampaign_tmp_impressions_and_spend_lj_campaign_gp_mappings AS a
    WHERE
        fb_mapped_campaign_group_name IS NULL );

CREATE TABLE
    IF NOT EXISTS gaintheory_us_targetusa_14.incampaign_facebook_campaign_names_to_map
    (
        fb_campaign_name VARCHAR(1000)
    );
INSERT
INTO
    gaintheory_us_targetusa_14.incampaign_facebook_log VALUES
    (
        'Facebook Extract: STEP 1',
        NOW(),
        'Extracted unmapped/new campaign names'
    );

MERGE
INTO
    gaintheory_us_targetusa_14.incampaign_facebook_campaign_names_to_map AS t
USING
    gaintheory_us_targetusa_14.incampaign_tmp_unmapped_campaign_names AS s
ON
    t.fb_campaign_name = s.fb_campaign_name
WHEN MATCHED
    THEN
UPDATE
SET
    fb_campaign_name = s.fb_campaign_name
WHEN NOT MATCHED
    THEN
INSERT
    (
        fb_campaign_name
    )
    VALUES
    (
        s.fb_campaign_name
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

