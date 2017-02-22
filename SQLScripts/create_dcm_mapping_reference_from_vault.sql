-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlDialectInspectionForFile
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_vault;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_vault AS
(
        SELECT
            m.*,
            n.creative,
            n.creative_id
        FROM
            (
                SELECT
                    k.*,
                    l.browser_platform
                FROM
                    (
                        SELECT
                            i.*,
                            j.operating_system
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
                                                    d.campaign
                                                FROM
                                                    (
                                                        SELECT
                                                            a.*,
                                                            b.placement
                                                        FROM
                                                            (
                                                                SELECT
                                                                    advertiser_id,
                                                                    campaign_id,
                                                                    placement_id,
                                                                    site_id_dcm,
                                                                    rendering_id,
                                                                    operating_system_id,
                                                                    browser_platform_id,
                                                                    COUNT(*) impr
                                                                FROM
                                                                    gaintheory_us_targetusa_14.TargetDFA2_impression
                                                                WHERE
                                                                    NEW_TIME(md_event_time, 'UTC',
                                                                    'EST')::DATE >= (GETDATE()-60)
                                                                    ::DATE
                                                                GROUP BY
                                                                    advertiser_id,
                                                                    campaign_id,
                                                                    placement_id,
                                                                    site_id_dcm,
                                                                    rendering_id,
                                                                    operating_system_id,
                                                                    browser_platform_id) a
                                                        LEFT JOIN
                                                            gaintheory_us_targetusa_14.TargetDFA2_placements
                                                            b
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
                        LEFT JOIN
                            gaintheory_us_targetusa_14.TargetDFA2_operating_systems j
                        ON
                            i.operating_system_id = j.operating_system_id) k
                LEFT JOIN
                    gaintheory_us_targetusa_14.TargetDFA2_browsers l
                ON
                    k.browser_platform_id = l.browser_platform_id) m
        LEFT JOIN
            gaintheory_us_targetusa_14.TargetDFA2_creatives n
        ON
            m.rendering_id = n.rendering_id
);
