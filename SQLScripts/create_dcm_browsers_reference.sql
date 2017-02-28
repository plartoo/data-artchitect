/*
Script to combine the browser info from DFA2 and DFA (old).
DFA2 browser information takes precedence over that of DFA (old).
This script should be run BEFORE 'create_dcm_mapping_reference_from_vault'
script because the latter uses 'incampaign_dfa_browsers_combined' to do 
its mapping.

Author: Phyo Thiha
Last Modified Date: Feb 27, 2017
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
