/*
This script generates mappings of Campaign, Channel, Device, Message,
Publisher and Tactic from DCM click and impression data.

Author: Phyo Thiha
Last Modified Date: Mar 31, 2017
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
            gaintheory_us_targetusa_14.TargetDFA2_impression AS a
        INNER JOIN
            gaintheory_us_targetusa_14.incampaign_dfa_operating_systems_combined AS b
        ON
            a.operating_system_id = b.operating_system_id
        INNER JOIN
            gaintheory_us_targetusa_14.incampaign_dfa_browsers_combined AS c
        ON
            a.browser_platform_id = c.browser_platform_id
        INNER JOIN
            gaintheory_us_targetusa_14.incampaign_dcm_os_browser_to_device_mappings AS d
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
            gaintheory_us_targetusa_14.TargetDFA2_click AS a
        INNER JOIN
            gaintheory_us_targetusa_14.incampaign_dfa_operating_systems_combined AS b
        ON
            a.operating_system_id = b.operating_system_id
        INNER JOIN
            gaintheory_us_targetusa_14.incampaign_dfa_browsers_combined AS c
        ON
            a.browser_platform_id = c.browser_platform_id
        INNER JOIN
            gaintheory_us_targetusa_14.incampaign_dcm_os_browser_to_device_mappings AS d
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
            from_impression_table ,
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
                    1 as from_impression_table,
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
                    0 as from_impression_table,
                    impr
                FROM
                    gaintheory_us_targetusa_14.incampaign_tmp_dcm_clicks_mapped_to_device ) AS a
        GROUP BY
            advertiser_id ,
            campaign_id ,
            placement_id ,
            site_id_dcm ,
            rendering_id ,
            device ,
            from_impression_table
    );


-- Mark stuff from IMPRS taable as 1
-- ONLY Calculate percentage based on that flag for message and then do the rest

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
                                                    from_impression_table ,
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
            v.from_impression_table AS dcm_from_impression_table ,
            v.impr                AS dcm_impr ,
            v.placement           AS dcm_placement ,
            v.placement_id        AS dcm_placement_id ,
            v.rendering_id        AS dcm_rendering_id ,
            v.site_dcm            AS dcm_site ,
            v.site_id_dcm         AS dcm_site_id ,
            p.AdserverCampaignId  AS prisma_adserver_campaign_id ,
            p.ProductName         AS prisma_product_name
        FROM
            gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_vault AS v
        LEFT JOIN
            (
                SELECT DISTINCT
                    AdserverCampaignId,
                    ProductName
                FROM
                    gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_prisma ) AS p
        ON
            v.campaign_id = p.AdserverCampaignId
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
            dcm_from_impression_table ,
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
                                        WHEN dcm_campaign = 'Email Impression Tracker'
                                        THEN 'Email Impression Tracker'
                                        WHEN dcm_campaign = '2016 Wedding (24255)'
                                        THEN 'WEDDING'
                                        WHEN dcm_campaign LIKE '%BidManager%'
                                        THEN 'BidManager'
                                        ELSE 'OTHERS'
                                    END
                    END
            END AS campaign
        FROM
            gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma
    );

/* Do the Publisher mapping */
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_publisher_mapped;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_publisher_mapped AS
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
            v.dcm_from_impression_table ,
            v.dcm_impr ,
            v.dcm_placement ,
            v.dcm_placement_id ,
            v.dcm_rendering_id ,
            v.dcm_site ,
            v.dcm_site_id ,
            v.prisma_adserver_campaign_id ,
            v.prisma_product_name ,
            v.campaign ,
            p.SupplierName        AS prisma_supplier_name ,
            CASE
                WHEN p.SupplierName IS NOT NULL
                THEN
                        CASE
                                WHEN v.dcm_site_id = '3104107' THEN 'TARGET-TMN'
                                WHEN v.dcm_site_id = '2784410' THEN 'TARGET-VIDEO'
                                WHEN v.dcm_site ilike '%bidmanager%' THEN 'BIDMANAGER'
                                ELSE
                                REGEXP_REPLACE( REGEXP_REPLACE( p.SupplierName,
                                    '[,\!\.]|(\sADV\s)|(COM)|(TV)|(LLC)|(INC)|(NETWORK)|(NTWK)', '', 1, 0, 'i') ,
                                    '\s*', '', 1, 0, 'i')
                        END
                ELSE 
                        CASE
                                WHEN v.dcm_site_id = '3104107' THEN 'TARGET-TMN'
                                WHEN v.dcm_site_id = '2784410' THEN 'TARGET-VIDEO'
                                WHEN v.dcm_site ilike '%bidmanager%' THEN 'BIDMANAGER'
                                ELSE
                                UPPER(CAST(REGEXP_REPLACE( REGEXP_REPLACE( v.dcm_site,
                                    '[,\!\.]|(\sADV\s)|(COM)|(TV)|(LLC)|(INC)|(NETWORK)|(NTWK)', '', 1, 0, 'i') ,
                                    '\s*', '', 1, 0, 'i') AS VARCHAR(500))) -- if we don't cast this to VARCHAR(500
                                    -- ), UPPER() wouldn't be happy worrying that it might cause character size
                                    -- overflow
                        END
            END AS publisher
        FROM
            gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_campaign_mapped AS v
            
        LEFT JOIN
            (
                SELECT DISTINCT -- we'll use DCM table's site names as reference
                    a.dcm_site_id, 
                    SupplierName
                FROM gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_campaign_mapped a
                INNER JOIN gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_prisma b
                ON a.dcm_placement_id = b.AdserverPlacementId) AS p
        ON
            v.dcm_site_id = p.dcm_site_id
    );

/* Do the Tactic mapping */
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_tactic_mapped;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_tactic_mapped AS
    (
        SELECT
            v.*,
            p.AdserverPlacementId AS prisma_adserver_placement_id ,
            p.TacticAttribution   AS prisma_tactic_attribution ,
            p.ChannelAttribution AS prisma_channel_attribution,
            CASE
                WHEN p.AdserverPlacementId IS NOT NULL
                THEN p.TacticAttribution
                ELSE 'Others'
            END AS tactic
        FROM
            gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_publisher_mapped AS v
        LEFT JOIN
            (
                SELECT DISTINCT
                    AdserverPlacementId,
                    TacticAttribution,
                    ChannelAttribution
                FROM gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_prisma ) AS p
        ON
            v.dcm_placement_id = p.AdserverPlacementId
    );

/* Do the Channel mapping */
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_channel_mapped;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_channel_mapped AS
    (
        SELECT
            v.*,
            COALESCE(prisma_channel_attribution,COALESCE(MAX(v.inferred_device) OVER (PARTITION BY v.dcm_placement_id), 'Crossdevice')) AS channel
        FROM
                (
                SELECT *
                , ((dcm_impr/ SUM(dcm_impr::float) OVER (PARTITION BY dcm_placement_id)) * 100) AS percentage_of_device_by_placement
                , 
                    CASE
                        WHEN (dcm_impr/ SUM(dcm_impr::float) OVER (PARTITION BY dcm_placement_id)) >= 0.95
                        THEN dcm_device
                    END AS inferred_device
                    FROM gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_tactic_mapped)
                    AS v
    );

/* Do the Message mapping
Step 1: assign Others and extract if it meets Manoj's criteria
*/
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_message_mapping_step1;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_message_mapping_step1 AS
    (
        SELECT
            v.*,
            CASE
                WHEN REGEXP_COUNT(v.dcm_creative,'\|') >= 4 THEN REGEXP_REPLACE(REGEXP_REPLACE(v.dcm_creative,'^(.+?)\|.*', '\1', 1, 0, 'i'),'\s|\(\d\)', '', 1, 0, 'i') -- removed spaces and '(1)' etc. per Manoj's request
                WHEN REGEXP_COUNT(v.dcm_creative,'_') >= 4 THEN REGEXP_REPLACE(REGEXP_REPLACE(v.dcm_creative,'^(.*?)_.*', '\1', 1, 0, 'i'),'\s|\(\d\)', '', 1, 0, 'i') -- removed spaces and '(1)' etc. per Manoj's request
                --WHEN v.dcm_creative = 'invisible.gif' THEN 'Site Served'
                ELSE 'Others'
        END AS message_draft
        FROM gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_channel_mapped AS v
    );

/* 
Step 2: Do the Message mapping
*/
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_message_mapping;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_message_mapping AS
    (
        SELECT
            c.*,
            (
                CASE
                    WHEN others_flag >= 1
                    THEN 'Others'
                    ELSE message_draft
                END) AS message
        FROM
            (
                SELECT
                    b.*,
                    SUM(
                        CASE
                            WHEN message_draft = 'Others'
                            AND percentage_of_impressions_by_campaign > 35.0 -- threshold set by Manoj
                            THEN 1
                            ELSE 0
                        END) over (partition BY campaign, dcm_from_impression_table) AS others_flag
                        --END) over (partition BY dcm_campaign_id) AS others_flag
                FROM
                    (
                        SELECT
                        a.*
                        , (SUM(dcm_impr::FLOAT) OVER (PARTITION BY campaign, message_draft, dcm_from_impression_table) / SUM(dcm_impr::FLOAT) OVER (PARTITION BY campaign, dcm_from_impression_table) * 100) AS percentage_of_impressions_by_campaign
--                        , (SUM(dcm_impr::FLOAT) OVER (PARTITION BY dcm_campaign_id, message_draft) / SUM(dcm_impr::FLOAT) OVER (PARTITION BY dcm_campaign_id) * 100) AS percentage_of_impressions_by_campaign
                        FROM gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_message_mapping_step1 AS a
                    ) b
             ) c
    );

DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_digital_metadata_impressions;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_digital_metadata_impressions  AS
    (
            SELECT DISTINCT
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
            FROM gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_message_mapping AS a
            WHERE a.dcm_from_impression_table = 1
    );

DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_digital_metadata_clicks;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_digital_metadata_clicks  AS
    (
            SELECT DISTINCT
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
            FROM gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_message_mapping AS a
            WHERE a.dcm_from_impression_table = 0
    );

/* Apply message info from impressions table to clicks table (only for the ones that can be matched by rendering_id. */
UPDATE gaintheory_us_targetusa_14.incampaign_tmp_digital_metadata_clicks t
SET message = s.message
FROM gaintheory_us_targetusa_14.incampaign_tmp_digital_metadata_impressions s
WHERE t.dcm_rendering_id = s.dcm_rendering_id;


/* Create final mapping table by combing both impression and click tables. */
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_digital_metadata;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_digital_metadata  AS
    (
        SELECT DISTINCT
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
                    FROM gaintheory_us_targetusa_14.incampaign_tmp_digital_metadata_clicks AS b
                    )
            ) c
    );

/* Clean up intermediate tables */
DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_impressions_mapped_to_device;
DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_clicks_mapped_to_device;
DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_clicks_and_impressions_combined;
DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_vault;
DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma;
DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_campaign_mapped;
DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_publisher_mapped;
DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_tactic_mapped;
DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_channel_mapped;
DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_lj_prisma_message_mapping_step1;

