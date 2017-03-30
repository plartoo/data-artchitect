
/*
Create impression table for last 60 days
*/
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_spend_tmp_dcm_impressions;

CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_spend_tmp_dcm_impressions AS
    (
        SELECT
            a.advertiser_id,
            a.campaign_id,
            NEW_TIME(a.md_event_time, 'UTC', 'EST')::DATE AS event_date,
            a.placement_id,
            a.site_id_dcm,
            a.rendering_id,
            COUNT(*)    impressions,
            0        AS clicks
        FROM
            gaintheory_us_targetusa_14.TargetDFA2_impression AS a
        WHERE
            NEW_TIME(a.md_event_time, 'UTC', 'EST')::DATE >= (GETDATE()-60) ::DATE
        GROUP BY
            advertiser_id,
            campaign_id,
            event_date,
            placement_id,
            site_id_dcm,
            rendering_id
    );

/*
Create click table for last 60 days
*/
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_spend_tmp_dcm_clicks;

CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_spend_tmp_dcm_clicks AS
    (
        SELECT
            a.advertiser_id,
            a.campaign_id,
            NEW_TIME(a.md_event_time, 'UTC', 'EST')::DATE AS event_date,
            a.placement_id,
            a.site_id_dcm,
            a.rendering_id,
            0        AS impressions,
            COUNT(*)    clicks
        FROM
            gaintheory_us_targetusa_14.TargetDFA2_click AS a
        WHERE
            NEW_TIME(a.md_event_time, 'UTC', 'EST')::DATE >= (GETDATE()-60) ::DATE
        GROUP BY
            advertiser_id,
            campaign_id,
            event_date,
            placement_id,
            site_id_dcm,
            rendering_id
    );

/*
Stack impression and click tables
*/
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_spend_tmp_dcm_clicks_and_impressions_combined;

CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_spend_tmp_dcm_clicks_and_impressions_combined AS
    (
        SELECT
            advertiser_id ,
            campaign_id ,
            event_date,
            placement_id ,
            site_id_dcm ,
            rendering_id ,
            SUM(impressions) AS impr,
            SUM(clicks)      AS clicks
        FROM
            (
                SELECT
                    advertiser_id ,
                    campaign_id ,
                    event_date,
                    placement_id ,
                    site_id_dcm ,
                    rendering_id ,
                    impressions,
                    clicks
                FROM
                    incampaign_spend_tmp_dcm_impressions
                UNION ALL
                SELECT
                    advertiser_id ,
                    campaign_id ,
                    event_date,
                    placement_id ,
                    site_id_dcm ,
                    rendering_id ,
                    impressions,
                    clicks
                FROM
                    incampaign_spend_tmp_dcm_clicks ) c
        GROUP BY
            advertiser_id ,
            campaign_id ,
            event_date,
            placement_id ,
            site_id_dcm ,
            rendering_id
    );
