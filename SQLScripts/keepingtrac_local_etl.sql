/*
ETL Script for KeepingTrac local data (Spot TV).
Author: Phyo Thiha
Last Modified: Feb 1, 2017
NOTE:
As of Jan 31, 2017, this code takes about 1903secs (32mins) total and outputs ~30k rows.
*/
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_keepingtrac_local_last_60_days;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_keepingtrac_local_last_60_days AS
    (
        SELECT
            zip              AS Geography ,
            'Target'         AS Product,
            Network          AS Campaign,
            'SpotTV_imp'     AS VariableName,
            ISCI_Length      AS Outlet,
            Cmml_Title       AS Creative,
            event_date       AS Period,
            SUM(Spot_TV_imp) AS VariableValue
        FROM
            (
                SELECT
                    b.zipcode AS zip,
                    a.Market_Code,
                    a.Network AS Network,
                    a.ISCI_Length,
                    a.event_date,
                    a.Cmml_Title,
                    a.Spot_TV_imp*b.wgt_pop AS Spot_TV_imp
                FROM
                    (
                        SELECT
                            Market_Code,
                            Network,
                            Station,
                            ISCI_Length,
                            CASE
                                WHEN Air_Time < '03:00:00'
                                THEN Air_Date + 1
                                ELSE Air_date
                            END AS event_date,
                            Cmml_Title,
                            SUM(Act_Impression) AS Spot_TV_imp
                        FROM
                            gaintheory_us_targetusa_14.incampaign_keepingtrac_local_all
                        WHERE
                            Type_of_Demographic = '2'
                        GROUP BY
                            Market_Code,
                            Network,
                            Station,
                            ISCI_Length,
                            event_date,
                            Cmml_Title ) a
                INNER JOIN
                    (
                        SELECT
                            a.ZipCode,
                            a.DMAC,
                            CASE
                                WHEN ( b.total_pop <> 0 )
                                THEN (CAST(a.Population AS FLOAT) / CAST(b.total_pop AS FLOAT) )
                                ELSE 0
                            END AS wgt_pop
                        FROM
                            gaintheory_us_targetusa_14.Facebook_DMA_to_zip a
                        LEFT JOIN
                            (
                                SELECT
                                    DMAC,
                                    SUM(Population) AS total_pop
                                FROM
                                    gaintheory_us_targetusa_14.Facebook_DMA_to_zip
                                GROUP BY
                                    DMAC) b
                        ON
                            a.DMAC = b.DMAC) b
                ON
                    a.Market_Code = b.DMAC-400) d
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
    ) ;
