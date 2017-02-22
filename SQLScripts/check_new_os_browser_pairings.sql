/* 
Extract all unique pairings of browser and os'es from current/latest vault DCM data.
Compare it against existing mapping table.
If new items are found, we'll notify the analysts.

Author: Phyo Thiha
Last Modified Date: Feb 17, 2017
*/
DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_all_os_and_browser_pairings;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_all_os_and_browser_pairings AS
    (
        SELECT DISTINCT
            browser_platform ,
            browser_platform_id ,
            operating_system ,
            operating_system_id
        FROM
            incampaign_dcm_mapping_reference_from_vault
    );
    

DROP TABLE
    IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_dcm_os_and_browser_to_map;
CREATE TABLE
    gaintheory_us_targetusa_14.incampaign_tmp_dcm_os_and_browser_to_map AS
    (
        SELECT
            browser_platform ,
            operating_system
        FROM
            incampaign_tmp_dcm_all_os_and_browser_pairings
        EXCEPT
        SELECT
            browser_platform ,
            operating_system
        FROM
            incampaign_dcm_os_browser_to_device_mappings -- table created via manual feed in DataVault
    );