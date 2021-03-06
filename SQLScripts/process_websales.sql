DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_websales_last_60_days;

INSERT
INTO
    gaintheory_us_targetusa_14.incampaign_websales_log VALUES
    (
        'Websales: STEP 1',
        NOW(),
        'Dropped old/existing last_60_days table'
    );

CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_websales_last_60_days AS -- ~ 4-5 million rows
    (
        SELECT
            'Geo_'||zipcode  AS Geography,
            'Target'         AS Product,
            'Total'         AS Campaign,
            'web_sales'      AS VariableName,
            'Total'          AS Outlet,
            'Total'          AS Creative,
            event_date::DATE AS Period,
            SUM(web_total_sales)   AS VariableValue
        FROM
            gaintheory_us_targetusa_14.incampaign_websales
        WHERE
            event_date >= (GETDATE()-60)::DATE
        GROUP BY
            Geography,
            Product,
            Campaign,
            VariableName,
            Outlet,
            Creative,
            Period
    );

INSERT
INTO
    gaintheory_us_targetusa_14.incampaign_websales_log VALUES
    (
        'Websales: STEP 2',
        NOW(),
        'Finished transform. Created table named incampaign_websales_last_60_days'
    );

COMMIT;


