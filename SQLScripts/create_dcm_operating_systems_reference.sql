/*
Script to combine the operating system info from DFA2 and DFA (old).
DFA2 os information takes precedence over that of DFA (old).
This script should be run BEFORE 'create_dcm_mapping_reference_from_vault'
script because the latter uses 'incampaign_dfa_operating_systems_combined'
to do its mapping.

Author: Phyo Thiha
Last Modified Date: Feb 27, 2017
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