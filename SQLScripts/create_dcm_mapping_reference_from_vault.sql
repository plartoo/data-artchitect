-- BEFORE this, we should run
--      create_dcm_browsers_reference
--      create_dcm_operating_systems_reference
--and
--      python script to find new device pairings
-- Then have a flag ready before this script is run
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_impressions_mapped_to_device;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_impressions_mapped_to_device AS
    (
        SELECT
            a.advertiser_id,
            a.campaign_id,
            a.placement_id,
            a.site_id_dcm,
            a.rendering_id,
            d.device,
            COUNT(*) impr
        FROM
            gaintheory_us_targetusa_14.TargetDFA2_impression a
        INNER JOIN
            gaintheory_us_targetusa_14.incampaign_dfa_operating_systems_combined b
        ON
            a.operating_system_id = b.operating_system_id
        INNER JOIN
            gaintheory_us_targetusa_14.incampaign_dfa_browsers_combined c
        ON
            a.browser_platform_id = c.browser_platform_id
        INNER JOIN
            gaintheory_us_targetusa_14.incampaign_dcm_os_browser_to_device_mappings d
        ON
            b.operating_system = d.operating_system
        AND c.browser_platform = d.browser_platform
        WHERE
            NEW_TIME(a.md_event_time, 'UTC', 'EST')::DATE >= (GETDATE()-60) ::DATE
        GROUP BY
            advertiser_id,
            campaign_id,
            placement_id,
            site_id_dcm,
            rendering_id,
            device
    );
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_clicks_mapped_to_device;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_clicks_mapped_to_device AS
    (
        SELECT
            a.advertiser_id,
            a.campaign_id,
            a.placement_id,
            a.site_id_dcm,
            a.rendering_id,
            d.device,
            COUNT(*) impr
        FROM
            gaintheory_us_targetusa_14.TargetDFA2_click a
        INNER JOIN
            gaintheory_us_targetusa_14.incampaign_dfa_operating_systems_combined b
        ON
            a.operating_system_id = b.operating_system_id
        INNER JOIN
            gaintheory_us_targetusa_14.incampaign_dfa_browsers_combined c
        ON
            a.browser_platform_id = c.browser_platform_id
        INNER JOIN
            gaintheory_us_targetusa_14.incampaign_dcm_os_browser_to_device_mappings d
        ON
            b.operating_system = d.operating_system
        AND c.browser_platform = d.browser_platform
        WHERE
            NEW_TIME(a.md_event_time, 'UTC', 'EST')::DATE >= (GETDATE()-60) ::DATE
        GROUP BY
            advertiser_id,
            campaign_id,
            placement_id,
            site_id_dcm,
            rendering_id,
            device
    );

-- Stack impressions and clicks results because Manoj said some placements
-- exclusively belong to these two tables
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_clicks_and_impressions_combined;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_clicks_and_impressions_combined AS
    (
        SELECT
            advertiser_id ,
            campaign_id ,
            placement_id ,
            site_id_dcm ,
            rendering_id ,
            device ,
            SUM(impr) AS impr
        FROM
            (
                SELECT
                    advertiser_id ,
                    campaign_id ,
                    placement_id ,
                    site_id_dcm ,
                    rendering_id ,
                    device ,
                    impr
                FROM
                    gaintheory_us_targetusa_14.incampaign_tmp_dcm_impressions_mapped_to_device
                UNION ALL
                SELECT
                    advertiser_id ,
                    campaign_id ,
                    placement_id ,
                    site_id_dcm ,
                    rendering_id ,
                    device ,
                    impr
                FROM
                    gaintheory_us_targetusa_14.incampaign_tmp_dcm_clicks_mapped_to_device ) a
        GROUP BY
            advertiser_id ,
            campaign_id ,
            placement_id ,
            site_id_dcm ,
            rendering_id ,
            device
    );
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_vault;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_vault AS
    (
        SELECT
            i.*,
            n.creative,
            n.creative_id
        FROM
--            (
--                SELECT
--                    k.*,
--                    l.browser_platform
--                FROM
--                    (
--                        SELECT
--                            i.*,
--                            j.operating_system
--                        FROM
                            (
                                SELECT
                                    g.*,
                                    h.site_dcm
                                FROM
                                    (
                                        SELECT
                                            e.*,
                                            f.advertiser
                                        FROM
                                            (
                                                SELECT
                                                    c.*,
                                                    d.campaign,
                                                    d.campaign_start_date,
                                                    d.campaign_end_date
                                                FROM
                                                    (
                                                        SELECT
                                                            a.*,
                                                            b.placement
                                                        FROM
                                                            (
                                                                SELECT
                                                                    advertiser_id ,
                                                                    campaign_id ,
                                                                    placement_id ,
                                                                    site_id_dcm ,
                                                                    rendering_id ,
                                                                    device ,
                                                                    impr
                                                                FROM
                                                                    gaintheory_us_targetusa_14.incampaign_tmp_dcm_clicks_and_impressions_combined
                                                            ) a
                                                        LEFT JOIN
                                                            gaintheory_us_targetusa_14.TargetDFA2_placements b
                                                        ON
                                                            a.placement_id = b.placement_id) c
                                                LEFT JOIN
                                                    gaintheory_us_targetusa_14.TargetDFA2_campaigns
                                                    d
                                                ON
                                                    c.campaign_id = d.campaign_id) e
                                        LEFT JOIN
                                            gaintheory_us_targetusa_14.TargetDFA2_advertisers f
                                        ON
                                            e.advertiser_id = f.advertiser_id) g
                                LEFT JOIN
                                    gaintheory_us_targetusa_14.TargetDFA2_sites h
                                ON
                                    g.site_id_dcm = h.site_id_dcm) i
--                        LEFT JOIN
--                            gaintheory_us_targetusa_14.TargetDFA2_operating_systems j
--                        ON
--                            i.operating_system_id = j.operating_system_id) k
--                LEFT JOIN
--                    gaintheory_us_targetusa_14.TargetDFA2_browsers l
--                ON
--                    k.browser_platform_id = l.browser_platform_id) m
        LEFT JOIN
            gaintheory_us_targetusa_14.TargetDFA2_creatives n
        ON
            i.rendering_id = n.rendering_id
        WHERE i.campaign NOT ilike '%Search%' -- Search is not our interest in this mapping task (Manoj Feb 28, 2017)
    );
--select distinct advertiser
--from incampaign_dcm_mapping_reference_from_vault

DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma AS
    (
        SELECT
            v.advertiser            AS dcm_advertiser ,
            v.advertiser_id         AS dcm_advertiser_id ,
            v.campaign              AS dcm_campaign ,
            v.campaign_id           AS dcm_campaign_id ,
                                                    v.campaign_start_date AS dcm_campaign_start_date,
                                                    v.campaign_end_date AS dcm_campaign_end_date,
            v.creative              AS dcm_creative ,
            v.creative_id           AS dcm_creative_id ,
            v.device                AS dcm_device,
            v.impr                  AS dcm_impr ,
            v.placement             AS dcm_placement ,
            v.placement_id          AS dcm_placement_id ,
            v.rendering_id          AS dcm_rendering_id ,
            v.site_dcm              AS dcm_site ,
            v.site_id_dcm           AS dcm_site_id ,
            p.AdserverCampaignId    AS prisma_adserver_campaign_id ,
--            p.AdserverPlacementId   AS prisma_adserver_placement_id ,
--            p.Channel               AS prisma_channel ,
--            p.ChannelAttribution    AS prisma_channel_attribution ,
--            p.ChannelType1          AS prisma_channel_type_1 ,
--            p.CostMethod            AS prisma_cost_method ,
--            p.InventoryType2        AS prisma_inventory_type_2 ,
--            p.PlacementStartDate    AS prisma_placement_start_date ,
--            p.PlacementEndDate      AS prisma_placement_end_date ,
            p.ProductName           AS prisma_product_name
--            p.Rate                  AS prisma_rate ,
--            p.RichMediaTypesFormat  AS prisma_rich_media_types_format ,
--            p.SupplierName          AS prisma_supplier_name ,
--            p.TacticAttribution     AS prisma_tactic_attribution ,
--            p.TargetingAudienceType AS prisma_targeting_audience_type
        FROM
            gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_vault AS v
        LEFT JOIN
        ( SELECT distinct AdserverCampaignId, ProductName FROM
            gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_prisma ) p
        ON
            v.campaign_id = p.AdserverCampaignId
--            AND
--            v.placement_id = p.AdserverPlacementId
--            AND 
--            v.dcm_site_id = p.AdserverSiteCode
    );

-- Campaign mapping begins
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_mapped;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_mapped AS
(
SELECT 
        dcm_advertiser
        ,dcm_advertiser_id
        ,dcm_campaign
        ,dcm_campaign_id
        ,dcm_campaign_start_date
        ,dcm_campaign_end_date
        ,dcm_creative
        ,dcm_creative_id
        ,dcm_device
        ,dcm_impr
        ,dcm_placement
        ,dcm_placement_id
        ,dcm_rendering_id
        ,dcm_site
        ,dcm_site_id
        ,prisma_adserver_campaign_id
--        ,prisma_adserver_placement_id
--        ,prisma_channel
--        ,prisma_channel_attribution
--        ,prisma_channel_type_1
--        ,prisma_cost_method
--        ,prisma_inventory_type_2
--        ,prisma_placement_start_date
--        ,prisma_placement_end_date
        ,prisma_product_name
--        ,prisma_rate
--        ,prisma_rich_media_types_format
--        ,prisma_supplier_name
--        ,prisma_tactic_attribution
--        ,prisma_targeting_audience_type
        , 
        CASE 
        WHEN prisma_adserver_campaign_id IS NOT NULL THEN prisma_product_name
        ELSE
                CASE
                        WHEN dcm_advertiser='Target - DVM' THEN 'DVM'
                        ELSE 
                                CASE 
                                        WHEN dcm_impr < 1000 THEN 'OTHERS'
                                        ELSE 'ToDo:MapManually'
                                END
                END
        END as campaign
--        ,
--        CASE 
--        WHEN prisma_adserver_placement_id IS NOT NULL 
--                THEN REGEXP_REPLACE(
--                        REGEXP_REPLACE(
--                                prisma_supplier_name, '[\.COM|\.TV|LLC|INC|NETWORK|\sADV\s]*', '', 1, 0, 'i')--'[\s!|\,|\.COM|\.TV|LLC|INC|NETWORK|\sADV\s]*', '', 1, 0, 'i')
--                        , '\s', '', 1, 0, 'i')
--        ELSE 
--                REGEXP_REPLACE(
--                        REGEXP_REPLACE(
--                                dcm_site, '[\.COM|\.TV|LLC|INC|NETWORK|\sADV\s]*', '', 1, 0, 'i') --'[\s!|\,|\.COM|\.TV|LLC|INC|NETWORK|\sADV\s]*', '', 1, 0, 'i')
--                        , '\s', '', 1, 0, 'i')||'__dcm_site'
--                
--                
----                (REGEXP_REPLACE(dcm_site, '\.[COM|TV]*', '', 1,0,'i'))||'__dcm_site'
--        END as publisher
--        ,
--        CASE 
--        WHEN prisma_adserver_placement_id IS NOT NULL THEN prisma_tactic_attribution
--        ELSE 
--                'Others'
--        END as tactic
FROM gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma
); /* Takes about 140 secs as of Feb 28, 2017*/
