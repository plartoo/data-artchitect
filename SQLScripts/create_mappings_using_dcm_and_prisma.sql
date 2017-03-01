/*
This script generates mappings of Campaign, Channel, Device, Message,
Publisher and Tactic from DCM click and impression data.

Author: Phyo Thiha
Last Modified Date: Feb 28, 2017
*/


/*
Combine the operating system info from DFA2 and DFA (old).
DFA2 os information takes precedence over that of DFA (old).
This script should be run BEFORE 'create_dcm_mapping_reference_from_vault'
script because the latter uses 'incampaign_dfa_operating_systems_combined'
to do its mapping.
*/
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_dfa_operating_systems_combined;

CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_dfa_operating_systems_combined AS
    (
        SELECT
            operating_system_id,
            operating_system
        FROM
            gaintheory_us_targetusa_14.TargetDFA2_operating_systems
    );

MERGE
INTO
    gaintheory_us_targetusa_14.incampaign_dfa_operating_systems_combined AS t
USING
    gaintheory_us_targetusa_14.dfa_operating_systems AS s
ON
    t.operating_system_id = s.os_id
WHEN NOT MATCHED
    THEN
INSERT
    (
        operating_system_id,
        operating_system
    )
    VALUES
    (
        s.os_id,
        s.os
    );

/*
Script to combine the browser info from DFA2 and DFA (old).
DFA2 browser information takes precedence over that of DFA (old).
This script should be run BEFORE 'create_dcm_mapping_reference_from_vault'
script because the latter uses 'incampaign_dfa_browsers_combined' to do 
its mapping.
*/
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_dfa_browsers_combined;

CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_dfa_browsers_combined AS
    (
        SELECT
            browser_platform_id,
            browser_platform
        FROM
            gaintheory_us_targetusa_14.TargetDFA2_browsers
    );

MERGE
INTO
    gaintheory_us_targetusa_14.incampaign_dfa_browsers_combined AS t
USING
    gaintheory_us_targetusa_14.dfa_browsers AS s
ON
    t.browser_platform_id = s.browser_id
WHEN NOT MATCHED
    THEN
INSERT
    (
        browser_platform_id,
        browser_platform
    )
    VALUES
    (
        s.browser_id,
        s.browser
    );

/* 
Create impression table with device info filled by using the OLD/EXISTING device mappings
*/
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

/* 
Create click table with device info filled by using the OLD/EXISTING device mappings
*/
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

/* 
Stack impression and click tables to cover ALL DCM campaigns and placements
*/
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

/* 
Create Vault aggregate base table that will be used to geenrate mappings
for Campaign, Channel, Message, Publisher and Tactic.
*/
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
                                    d.campaign_start_date,      -- included for QA if needed
                                    d.campaign_end_date         -- included for QA if needed
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
                                    gaintheory_us_targetusa_14.TargetDFA2_campaigns d
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
        LEFT JOIN
            gaintheory_us_targetusa_14.TargetDFA2_creatives n
        ON
            i.rendering_id = n.rendering_id
        WHERE
            i.campaign NOT ilike '%Search%' -- Search is not our interest in this mapping task (Manoj Feb 28, 2017)
    );

DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma AS
    (
        SELECT
            v.advertiser          AS dcm_advertiser ,
            v.advertiser_id       AS dcm_advertiser_id ,
            v.campaign            AS dcm_campaign ,
            v.campaign_id         AS dcm_campaign_id ,
            v.campaign_start_date AS dcm_campaign_start_date,
            v.campaign_end_date   AS dcm_campaign_end_date,
            v.creative            AS dcm_creative ,
            v.creative_id         AS dcm_creative_id ,
            v.device              AS dcm_device,
            v.impr                AS dcm_impr ,
            v.placement           AS dcm_placement ,
            v.placement_id        AS dcm_placement_id ,
            v.rendering_id        AS dcm_rendering_id ,
            v.site_dcm            AS dcm_site ,
            v.site_id_dcm         AS dcm_site_id ,
            p.AdserverCampaignId  AS prisma_adserver_campaign_id ,
            p.ProductName         AS prisma_product_name
            --            p.Channel               AS prisma_channel ,
            --            p.ChannelAttribution    AS prisma_channel_attribution ,
            --            p.ChannelType1          AS prisma_channel_type_1 ,
            --            p.CostMethod            AS prisma_cost_method ,
            --            p.InventoryType2        AS prisma_inventory_type_2 ,
            --            p.PlacementStartDate    AS prisma_placement_start_date ,
            --            p.PlacementEndDate      AS prisma_placement_end_date ,
            --            p.Rate                  AS prisma_rate ,
            --            p.RichMediaTypesFormat  AS prisma_rich_media_types_format ,
            --            p.TargetingAudienceType AS prisma_targeting_audience_type
        FROM
            gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_vault AS v
        LEFT JOIN
            (
                SELECT DISTINCT
                    AdserverCampaignId,
                    ProductName
                FROM
                    gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_prisma ) p
        ON
            v.campaign_id = p.AdserverCampaignId
            --            AND
            --            v.dcm_site_id = p.AdserverSiteCode
    );

/* Do the Campaign mapping */
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_campaign_mapped;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_campaign_mapped AS
    (
        SELECT
            dcm_advertiser ,
            dcm_advertiser_id ,
            dcm_campaign ,
            dcm_campaign_id ,
            dcm_campaign_start_date ,
            dcm_campaign_end_date ,
            dcm_creative ,
            dcm_creative_id ,
            dcm_device ,
            dcm_impr ,
            dcm_placement ,
            dcm_placement_id ,
            dcm_rendering_id ,
            dcm_site ,
            dcm_site_id ,
            prisma_adserver_campaign_id ,
            prisma_product_name ,
            CASE
                WHEN prisma_adserver_campaign_id IS NOT NULL
                THEN prisma_product_name
                ELSE
                    CASE
                        WHEN dcm_advertiser='Target - DVM'
                        THEN 'DVM'
                        ELSE
                            CASE
                                WHEN dcm_impr < 1000
                                THEN 'OTHERS'
                                ELSE
                                    CASE
                                        WHEN dcm_campaign = 'Email Impression Tarcker'
                                        THEN 'Email Impression Tarcker'
                                        WHEN dcm_campaign = '2016 Wedding (24255)'
                                        THEN 'WEDDING'
                                        WHEN dcm_campaign LIKE '%BidManager%'
                                        THEN 'BidManager'
                                        ELSE 'OTHERS'
                                    END
                            END
                    END
            END AS campaign
        FROM
            gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma
    );

/* Do the Publisher and Tactic mapping */
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_publisher_tactic_mapped;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_publisher_tactic_mapped AS
    (
        SELECT
            v.dcm_advertiser ,
            v.dcm_advertiser_id ,
            v.dcm_campaign ,
            v.dcm_campaign_id ,
            v.dcm_campaign_start_date ,
            v.dcm_campaign_end_date ,
            v.dcm_creative ,
            v.dcm_creative_id ,
            v.dcm_device ,
            v.dcm_impr ,
            v.dcm_placement ,
            v.dcm_placement_id ,
            v.dcm_rendering_id ,
            v.dcm_site ,
            v.dcm_site_id ,
            v.prisma_adserver_campaign_id ,
            v.prisma_product_name ,
            v.campaign ,
            p.AdserverPlacementId AS prisma_adserver_placement_id ,
            p.SupplierName        AS prisma_supplier_name ,
            p.TacticAttribution   AS prisma_tactic_attribution ,
            CASE
                WHEN p.AdserverPlacementId IS NOT NULL
                THEN REGEXP_REPLACE( REGEXP_REPLACE( p.SupplierName,
                    '[,\!\.]|(\sADV\s)|(COM)|(TV)|(LLC)|(INC)|(NETWORK)|(NTWK)', '', 1, 0, 'i') ,
                    '\s*', '', 1, 0, 'i')
                ELSE UPPER(CAST(REGEXP_REPLACE( REGEXP_REPLACE( v.dcm_site,
                    '[,\!\.]|(\sADV\s)|(COM)|(TV)|(LLC)|(INC)|(NETWORK)|(NTWK)', '', 1, 0, 'i') ,
                    '\s*', '', 1, 0, 'i') AS VARCHAR(500))) -- if we don't cast this to VARCHAR(500
                    -- ), UPPER() wouldn't be happy worrying that it might cause character size
                    -- overflow
            END AS publisher ,
            CASE
                WHEN p.AdserverPlacementId IS NOT NULL
                THEN p.TacticAttribution
                ELSE 'Others'
            END AS tactic
        FROM
            gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_campaign_mapped AS v
        LEFT JOIN
            (
                SELECT DISTINCT
                    AdserverPlacementId,
                    SupplierName,
                    TacticAttribution
                FROM
                    gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_prisma ) p
        ON
            v.dcm_placement_id = p.AdserverPlacementId
    );

