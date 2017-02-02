/*
ETL Script for DCM impression data.
Author: Phyo Thiha
Last Modified: Jan 31, 2017
NOTE:
As of Jan 31, 2017, this code takes about 1903secs (32mins) total and outputs ~82.2m rows.
*/
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_target_attribution_meta_cons;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_target_attribution_meta_cons AS
    (
        SELECT DISTINCT
            page_id, --page_id is now placement_id in DCM 2.0
            creative_id, -- creative_id is now rendering_id in DCM 2.0
            LOWER(
                CASE
                    WHEN campaign_publisher_channel_tactic_message ilike '%Style_Digital%'
                    THEN SPLIT_PART(campaign_publisher_channel_tactic_message,'_',2 )||SPLIT_PART
                        (campaign_publisher_channel_tactic_message,'_' ,3)
                    ELSE SPLIT_PART(campaign_publisher_channel_tactic_message,'_',2 )
                END) AS campaign,
            LOWER(
                CASE
                    WHEN campaign_publisher_channel_tactic_message ilike '%Style_Digital%'
                    THEN SPLIT_PART(campaign_publisher_channel_tactic_message,'_',4 )
                    ELSE SPLIT_PART(campaign_publisher_channel_tactic_message,'_',3 )
                END) AS publisher,
            LOWER(
                CASE
                    WHEN campaign_publisher_channel_tactic_message ilike '%Style_Digital%'
                    THEN SPLIT_PART(campaign_publisher_channel_tactic_message,'_',5 )
                    ELSE SPLIT_PART(campaign_publisher_channel_tactic_message,'_',4 )
                END) AS channel,
            LOWER(
                CASE
                    WHEN campaign_publisher_channel_tactic_message ilike '%Style_Digital%'
                    THEN SPLIT_PART(campaign_publisher_channel_tactic_message,'_',6 )
                    ELSE SPLIT_PART(campaign_publisher_channel_tactic_message,'_',5 )
                END) AS tactic,
            LOWER(
                CASE
                    WHEN campaign_publisher_channel_tactic_message ilike '%Style_Digital%'
                    THEN SPLIT_PART(campaign_publisher_channel_tactic_message,'_',7 )
                    ELSE SPLIT_PART(campaign_publisher_channel_tactic_message,'_',6 )
                END) AS MESSAGE
        FROM
            gaintheory_us_targetusa_14.campaign_target_attribution_meta_cons
    ); -- ~2 secs with 48K rows


DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_dfa_impressions;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_dfa_impressions AS
    (
        SELECT
            'Geo'||zip_postal_code AS Geography,
            'Target'               AS Product,
            'Total'                AS Campaign,
            'Total_Display_Imp'    AS VariableName,
            'Total'                AS Outlet,
            'Total'                AS Creative,
            event_date             AS Period,
            SUM(impressions)       AS VariableValue
        FROM
            (
                SELECT
                    zip_postal_code,
                    a.md_event_time::DATE AS event_date,
                    COUNT(*)              AS impressions
                FROM
                    gaintheory_us_targetusa_14.TargetDFA2_impression a
                LEFT JOIN
                    gaintheory_us_targetusa_14.incampaign_tmp_target_attribution_meta_cons b
                ON
                    a.placement_id = b.page_id
                AND a.rendering_id = b.creative_id
                WHERE
                    a.md_event_time::DATE >= (GETDATE()-60)::DATE
                GROUP BY
                    event_date,
                    zip_postal_code ) c
        GROUP BY
            Geography,
            Product,
            Campaign,
            VariableName,
            Outlet,
            Creative,
            Period
    ) -- ~19 secs; 1.525m rows
UNION ALL
    (
        SELECT
            'Geo'||zip_postal_code AS Geography,
            'Target'               AS Product,
            rawcamp                AS Campaign,
            'NonDVM_Display_Imp'   AS VariableName,
            publisher||'_'||tactic AS Outlet,
            channel||'_'||MESSAGE  AS Creative,
            event_date             AS Period,
            SUM(impressions)       AS VariableValue
        FROM
            (
                SELECT
                    zip_postal_code,
                    CASE
                        WHEN b.campaign = 'styledigital'
                        THEN 'style'
                        ELSE b.campaign
                    END                   AS rawcamp,
                    b.channel             AS channel,
                    b.message             AS MESSAGE,
                    a.md_event_time::DATE AS event_date,
                    COUNT(*)              AS impressions,
                    b.publisher,
                    b.tactic
                FROM
                    gaintheory_us_targetusa_14.TargetDFA2_impression a
                LEFT JOIN
                    gaintheory_us_targetusa_14.incampaign_tmp_target_attribution_meta_cons b
                ON
                    a.placement_id = b.page_id
                AND a.rendering_id = b.creative_id
                WHERE
                    a.md_event_time::DATE >= (GETDATE()-60)::DATE
                AND b.campaign NOT ilike '%DVM%'
                GROUP BY
                    event_date,
                    b.campaign,
                    b.publisher,
                    b.channel,
                    b.tactic,
                    b.message,
                    zip_postal_code ) c
        GROUP BY
            Geography,
            Product,
            Campaign,
            VariableName,
            Outlet,
            Creative,
            Period )
UNION ALL
    (
        SELECT
            'Geo'||zip_postal_code AS Geography,
            'Target'               AS Product,
            rawcamp                AS Campaign,
            'DVM_Display_Imp'      AS VariableName,
            publisher||'_'||tactic AS Outlet,
            channel||'_'||MESSAGE  AS Creative,
            event_date             AS Period,
            SUM(impressions)       AS VariableValue
        FROM
            (
                SELECT
                    zip_postal_code,
                    CASE
                        WHEN b.campaign = 'styledigital'
                        THEN 'style'
                        ELSE b.campaign
                    END AS rawcamp,
                    b.channel,
                    b.message,
                    a.md_event_time::DATE AS event_date,
                    COUNT(*)              AS impressions,
                    b.publisher,
                    b.tactic
                FROM
                    gaintheory_us_targetusa_14.TargetDFA2_impression a
                LEFT JOIN
                    gaintheory_us_targetusa_14.incampaign_tmp_target_attribution_meta_cons b
                ON
                    a.placement_id = b.page_id
                AND a.rendering_id = b.creative_id
                WHERE
                    a.md_event_time::DATE >= (GETDATE()-60)::DATE
                AND b.campaign ilike '%DVM%'
                GROUP BY
                    event_date,
                    b.campaign,
                    b.publisher,
                    b.channel,
                    b.tactic,
                    b.message,
                    zip_postal_code ) c
        GROUP BY
            Geography,
            Product,
            Campaign,
            VariableName,
            Outlet,
            Creative,
            Period );
            
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_target_attribution_meta_cons;