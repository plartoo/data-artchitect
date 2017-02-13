/*
Script to stack all last_60_days tables as an IRT table.

Author: Phyo Thiha
Last Modified: Feb 13, 2017

NOTE:
As of Jan 31, 2017, this code takes about 670 secs to finish and outputs 235m rows.
*/

DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_IRT_all_last_60_days;

CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_IRT_all_last_60_days AS
        SELECT
            incampaign_dfa_clicks_last_60_days.Geography,
            incampaign_dfa_clicks_last_60_days.Product,
            incampaign_dfa_clicks_last_60_days.Campaign,
            incampaign_dfa_clicks_last_60_days.VariableName,
            incampaign_dfa_clicks_last_60_days.Outlet,
            incampaign_dfa_clicks_last_60_days.Creative,
            incampaign_dfa_clicks_last_60_days.Period,
            incampaign_dfa_clicks_last_60_days.VariableValue
        FROM
            gaintheory_us_targetusa_14.incampaign_dfa_clicks_last_60_days
        UNION ALL
        SELECT
            incampaign_dfa_impressions_last_60_days.Geography,
            incampaign_dfa_impressions_last_60_days.Product,
            incampaign_dfa_impressions_last_60_days.Campaign,
            incampaign_dfa_impressions_last_60_days.VariableName,
            incampaign_dfa_impressions_last_60_days.Outlet,
            incampaign_dfa_impressions_last_60_days.Creative,
            incampaign_dfa_impressions_last_60_days.Period,
            incampaign_dfa_impressions_last_60_days.VariableValue
        FROM
            gaintheory_us_targetusa_14.incampaign_dfa_impressions_last_60_days
        UNION ALL
        SELECT
            incampaign_facebook_last_60_days.Geography,
            incampaign_facebook_last_60_days.Product,
            incampaign_facebook_last_60_days.Campaign,
            incampaign_facebook_last_60_days.VariableName,
            incampaign_facebook_last_60_days.Outlet,
            incampaign_facebook_last_60_days.Creative,
            incampaign_facebook_last_60_days.Period,
            incampaign_facebook_last_60_days.VariableValue
        FROM
            gaintheory_us_targetusa_14.incampaign_facebook_last_60_days
        UNION ALL
        SELECT
            incampaign_keepingtrac_local_last_60_days.Geography,
            incampaign_keepingtrac_local_last_60_days.Product,
            incampaign_keepingtrac_local_last_60_days.Campaign,
            incampaign_keepingtrac_local_last_60_days.VariableName,
            (incampaign_keepingtrac_local_last_60_days.Outlet)::VARCHAR AS Outlet,
            incampaign_keepingtrac_local_last_60_days.Creative,
            incampaign_keepingtrac_local_last_60_days.Period,
            incampaign_keepingtrac_local_last_60_days.VariableValue
        FROM
            gaintheory_us_targetusa_14.incampaign_keepingtrac_local_last_60_days
        UNION ALL
        SELECT
            incampaign_rentrak_last_60_days.Geography,
            incampaign_rentrak_last_60_days.Product,
            incampaign_rentrak_last_60_days.Campaign,
            incampaign_rentrak_last_60_days.VariableName,
            incampaign_rentrak_last_60_days.Outlet,
            incampaign_rentrak_last_60_days.Creative,
            incampaign_rentrak_last_60_days.Period,
            incampaign_rentrak_last_60_days.VariableValue
        FROM
            gaintheory_us_targetusa_14.incampaign_rentrak_last_60_days
        UNION ALL
        SELECT
            incampaign_storesales_last_60_days.Geography,
            incampaign_storesales_last_60_days.Product,
            incampaign_storesales_last_60_days.Campaign,
            incampaign_storesales_last_60_days.VariableName,
            incampaign_storesales_last_60_days.Outlet,
            incampaign_storesales_last_60_days.Creative,
            incampaign_storesales_last_60_days.Period,
            incampaign_storesales_last_60_days.VariableValue
        FROM
            gaintheory_us_targetusa_14.incampaign_storesales_last_60_days
        UNION ALL
        SELECT
            incampaign_websales_last_60_days.Geography,
            incampaign_websales_last_60_days.Product,
            incampaign_websales_last_60_days.Campaign,
            incampaign_websales_last_60_days.VariableName,
            incampaign_websales_last_60_days.Outlet,
            incampaign_websales_last_60_days.Creative,
            incampaign_websales_last_60_days.Period,
            incampaign_websales_last_60_days.VariableValue
        FROM
            gaintheory_us_targetusa_14.incampaign_websales_last_60_days
;