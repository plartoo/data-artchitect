/*
ETL Script for DCM impression data.
Author: Phyo Thiha
Last Modified: Jan 31, 2017
NOTE:
As of Jan 31, 2017, this code takes about 1903secs (32mins) total and outputs ~82.2m rows.
*/

DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_dfa_impressions_last_60_days;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_dfa_impressions_last_60_days AS
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
                    NEW_TIME(a.md_event_time, 'UTC', 'EST')::DATE AS event_date,
                    COUNT(*)              AS impressions
                FROM
                    gaintheory_us_targetusa_14.TargetDFA2_impression a
                LEFT JOIN
                    gaintheory_us_targetusa_14.incampaign_digital_metadata b
                ON
                    a.placement_id = b.dcm_placement_id
                AND a.rendering_id = b.dcm_rendering_id
                WHERE
                    NEW_TIME(a.md_event_time, 'UTC', 'EST')::DATE >= (GETDATE()-60)::DATE
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
                    NEW_TIME(a.md_event_time, 'UTC', 'EST')::DATE AS event_date,
                    COUNT(*)              AS impressions,
                    b.publisher,
                    b.tactic
                FROM
                    gaintheory_us_targetusa_14.TargetDFA2_impression a
                LEFT JOIN
                    gaintheory_us_targetusa_14.incampaign_digital_metadata b
                ON
                    a.placement_id = b.dcm_placement_id
                AND a.rendering_id = b.dcm_rendering_id
                WHERE
                    NEW_TIME(a.md_event_time, 'UTC', 'EST')::DATE >= (GETDATE()-60)::DATE
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
                    NEW_TIME(a.md_event_time, 'UTC', 'EST')::DATE AS event_date,
                    COUNT(*)              AS impressions,
                    b.publisher,
                    b.tactic
                FROM
                    gaintheory_us_targetusa_14.TargetDFA2_impression a
                LEFT JOIN
                    gaintheory_us_targetusa_14.incampaign_digital_metadata b
                ON
                    a.placement_id = b.dcm_placement_id
                AND a.rendering_id = b.dcm_rendering_id
                WHERE
                    NEW_TIME(a.md_event_time, 'UTC', 'EST')::DATE >= (GETDATE()-60)::DATE
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
            
