DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_storesales_last_60_days;

INSERT
INTO
    gaintheory_us_targetusa_14.incampaign_storesales_log VALUES
    (
        'Storesales: STEP 1',
        NOW(),
        'Dropped old/existing last_60_days table'
    );

CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_storesales_last_60_days AS -- ~ 4-5 million rows
    (
        SELECT
            'Geo_'||zipcode  AS Geography,
            'Target'         AS Product,
            'Total'         AS Campaign,
            'store_sales'      AS VariableName,
            'Total'          AS Outlet,
            'Total'          AS Creative,
            event_date::DATE AS Period,
            SUM(store_total_sales)   AS VariableValue
        FROM
            gaintheory_us_targetusa_14.incampaign_storesales
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
    gaintheory_us_targetusa_14.incampaign_storesales_log VALUES
    (
        'Storesales: STEP 2',
        NOW(),
        'Finished transform. Created table named incampaign_storesales_last_60_days'
    );

COMMIT;


