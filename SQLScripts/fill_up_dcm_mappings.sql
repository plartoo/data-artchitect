DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma AS
    (
        SELECT
            v.advertiser            AS dcm_advertiser ,
            v.advertiser_id         AS dcm_advertiser_id ,
--            v.browser_platform      AS dcm_browser_platform ,
--            v.browser_platform_id   AS dcm_browser_platform_id ,
            v.campaign              AS dcm_campaign ,
            v.campaign_id           AS dcm_campaign_id ,
            v.creative              AS dcm_creative ,
            v.creative_id           AS dcm_creative_id ,
            v.impr                  AS dcm_impr ,
--            v.operating_system      AS dcm_operating_system ,
--            v.operating_system_id   AS dcm_operating_system_id ,
            v.placement             AS dcm_placement ,
            v.placement_id          AS dcm_placement_id ,
            v.rendering_id          AS dcm_rendering_id ,
            v.site_dcm              AS dcm_site ,
            v.site_id_dcm           AS dcm_site_id ,
            p.AdserverCampaignId    AS prisma_adserver_campaign_id ,
            p.AdserverPlacementId   AS prisma_adserver_placement_id ,
            p.Channel               AS prisma_channel ,
            p.ChannelAttribution    AS prisma_channel_attribution ,
            p.ChannelType1          AS prisma_channel_type_1 ,
            p.CostMethod            AS prisma_cost_method ,
            p.InventoryType2        AS prisma_inventory_type_2 ,
            p.PlacementStartDate    AS prisma_placement_start_date ,
            p.PlacementEndDate      AS prisma_placement_end_date ,
            p.ProductName           AS prisma_product_name ,
            p.Rate                  AS prisma_rate ,
            p.RichMediaTypesFormat  AS prisma_rich_media_types_format ,
            p.SupplierName          AS prisma_supplier_name ,
            p.TacticAttribution     AS prisma_tactic_attribution ,
            p.TargetingAudienceType AS prisma_targeting_audience_type
        FROM
            gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_vault AS v
        LEFT JOIN
            gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_prisma p
        ON
            v.campaign_id = p.AdserverCampaignId
--            AND 
--            v.dcm_site_id = p.AdserverSiteCode
--            AND
--            v.dcm_placement_id = p.AdserverPlacementId
    );/* Takes about 80 secs and outputs ~41m rows */

select distinct
operating_system_id,
browser_platform_id,
browser_platform,
operating_system
from incampaign_dcm_mapping_reference_from_vault
order by 3, 4

DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_final;
    
SELECT
    *
    ,
    CASE 
    WHEN prisma_adserver_campaign_id IS NOT NULL THEN prisma_product_name
    ELSE 
        CASE
                WHEN dcm_advertiser='Target - DVM' THEN 'DVM'
                ELSE 
                        CASE 
                                WHEN dcm_impr<=1000 THEN 'Others'
                                ELSE 'ToDo:MapManually'
                        END

        END
    END as campaign
    ,'channel' as channel
    ,'publisher' as publisher
    ,'message' as message
    ,'tactic' as tactic
INTO
    gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_final 
FROM
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma 
;


incampaign_tmp_dcm_lj_prisma