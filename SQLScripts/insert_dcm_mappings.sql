DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_digital_metadata;
    
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_digital_metadata 
    (
        campaign                varchar(100),
        channel                 varchar(100),
        dcm_advertiser_id       integer,
        dcm_campaign_id         integer,
        dcm_creative_id         integer,
        dcm_placement_id        integer,
        dcm_rendering_id        integer,
        dcm_site_id             integer,
        message                 varchar(200),
        message_draft           varchar(200),
        publisher               varchar(100),
        tactic                  varchar(100)
);

INSERT INTO
    gaintheory_us_targetusa_14.incampaign_digital_metadata 
     (
        SELECT DISTINCT
         INITCAPB(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(campaign,'/'),'&'),' '),','),'-'),'\''')) as campaign
        ,INITCAPB(REGEXP_REPLACE(channel,' ')) as channel
        ,dcm_advertiser_id
        ,dcm_campaign_id
        ,dcm_creative_id
        ,dcm_placement_id
        ,dcm_rendering_id
        ,dcm_site_id
        ,REGEXP_REPLACE(message,'\''') as message
        ,message_draft
        ,REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
        case 
                when publisher = 'DISNEYONLINE' then 'DISNEY'
                when publisher = 'EVERYDAYHEALTHMEDIA' then 'EVERYDAYHEALTH'
                when publisher = 'FOX' then 'FOXAUDIENCE'
                when publisher = 'GLAMMEDIA' then 'GLAM'
                when publisher = 'SCRIPPSS' then 'SCRIPPS'
         else publisher end , '\–'), '\+'), '\/'),'\-') as publisher
        ,tactic
        FROM (
                    (
                    SELECT 
                        campaign
                        ,channel
                        ,dcm_advertiser_id
                        ,dcm_campaign_id
                        ,dcm_creative_id
                        ,dcm_from_impression_table
                        ,dcm_placement_id
                        ,dcm_rendering_id
                        ,dcm_site_id
                        ,message
                        ,message_draft
                        ,publisher
                        ,tactic
                    FROM gaintheory_us_targetusa_14.incampaign_tmp_digital_metadata_impressions AS a
                    )
                    UNION ALL 
                    (
                    SELECT 
                        campaign
                        ,channel
                        ,dcm_advertiser_id
                        ,dcm_campaign_id
                        ,dcm_creative_id
                        ,dcm_from_impression_table
                        ,dcm_placement_id
                        ,dcm_rendering_id
                        ,dcm_site_id
                        ,message
                        ,message_draft
                        ,publisher
                        ,tactic
                    FROM gaintheory_us_targetusa_14.incampaign_tmp_digital_metadata_clicks AS b
                    )
            ) c
    );
COMMIT;