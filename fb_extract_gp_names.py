from trigger_on_row_count_change import *


def get_msg_body_for_completion():
    mapping_table = 'incampaign_facebook_campaign_group_names_to_map'
    process_name = 'fb_etl_and_export_to_s3'
    return """
        <p>Python script just extracted campaign group names from new fb data loaded to Vault recently.</p>
        <p>To run the rest of the ETL process, please do the followings:
        <ol>
        <li>download the attached file in this email: <b>{0}</b></li>
        <li>create mappings for campaign names (under column A) in column B</li>
        <li>upload that updated mapping CSV file via 'InCampaign_Facebook_Campaign_Group_Name_Mapping' feed in DataVault</li>
        <li><strong style="color: red;">AFTER DataVault load above is successful</strong>
            run this SQL in Vertica backend: <br>
            <b>
            UPDATE gaintheory_us_targetusa_14.incampaign_process_switches
            SET run = 1
            WHERE process_name = '{1}';
            </b>
        </li>
        </ol>
        </p><br>
        <p><strong style="color: red;">NOTE: If you fail to run UPDATE SQL query as directed above, the second
        part of facebook data will not be processed.</strong></p>
        """.format(mapping_table, process_name)


def main():
    table_and_actions = {
        'incampaign_facebook_impressions_and_spend':
            [
                {'cmd': ['python', ROOT_FOLDER+'run_vsql.py', SQL_SCRIPT_FOLDER+'fb_extract_gp_names.sql'],
                  # 'notify_on_complete': {
                  #     'subject': 'InCampaign Facebook: new campaign group names are extracted (Follow up action needed)',
                  #     'body': get_msg_body_for_completion(),
                  #     'recipients': ONSHORE_EMAIL_RECIPIENTS}
                 },
                # {'cmd': ['python', ROOT_FOLDER + 'run_vsql.py', SQL_SCRIPT_FOLDER + 'fb_extract_gp_names.sql']}

            ]
    }
    trigger_on_row_count_change(table_and_actions)

if __name__ == "__main__":
    main()
