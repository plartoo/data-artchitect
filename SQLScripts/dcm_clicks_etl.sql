/*
ETL Script for DCM click data.

Author: Phyo Thiha
Last Modified: Jan 31, 2017

NOTE:
As of Jan 31, 2017, this code takes about 366 secs to finish and outputs 14.13m rows.
*/

DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dfa2_clicks_subset;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dfa2_clicks_subset AS
    (
        SELECT
            x.placement_id,
            x.zip_postal_code,
            NEW_TIME(x.md_event_time, 'UTC', 'EST')::DATE AS event_date,
            COUNT(*)                      AS clicks
        FROM
            gaintheory_us_targetusa_14.TargetDFA2_click x
        WHERE
            x.advertiser_id = '2906542'
        AND x.campaign_id IN ('9009889',
                              '9640480')
        AND NEW_TIME(x.md_event_time, 'UTC', 'EST')::DATE >= (GETDATE()-60)::DATE
        GROUP BY
            placement_id,
            zip_postal_code,
            event_date
    );

DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_dfa_clicks_last_60_days;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_dfa_clicks_last_60_days AS
    (
        SELECT
            'Geo'||zip_postal_code AS Geography,
            'Target'               AS Product,
            CASE
                WHEN position('Search' IN placement) = 0
                AND position('(' IN placement) = 0
                THEN REGEXP_REPLACE(REGEXP_REPLACE(placement,'[^\w\s]',''),'[ ]+', '') -- get anything inside parens like "Target Bulk 39 (Mobile+Phones) => Mobile+Phones"
                ELSE -- or "Video+Games+Search => VideoGames"
                    CASE
                        WHEN position('Search' IN placement) = 0
                        THEN REGEXP_REPLACE(REGEXP_REPLACE(SPLIT_PART(placement,'(',2),'[^\w\s]',''
                            ),'[ ]+' , '')
                        ELSE REGEXP_REPLACE(REGEXP_REPLACE(LEFT(placement, position('Search' IN
                            placement) -1),'[^\w\s]',''),'[ ]+', '')
                    END
            END          AS Campaign,
            'Search_click'         AS VariableName,
            'Total'                AS Outlet,
            'Total'                AS Creative,
            c.event_date AS Period,
            SUM(clicks)  AS VariableValue
        FROM
            gaintheory_us_targetusa_14.incampaign_tmp_dfa2_clicks_subset c
        JOIN
            gaintheory_us_targetusa_14.TargetDFA2_placements d
        ON
            c.placement_id = d.placement_id
        GROUP BY
            Geography,
            Product,
            Campaign,
            VariableName,
            Outlet,
            Creative,
            Period
    );

DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dfa2_clicks_subset;