from trigger_on_flag_value_change import *
from vertica_utils import *


def remind_to_truncate_table(table, flag):
    return """
        <p>Seems like mapping table(s), <b>{0}</b>, is/are not yet truncated.
        In order to proceed with the rest of Facebook ETL processing,
        we need you to do all of the following steps:</p>
        <ol>
        <li>download <b>{0}</b> table(s) from Vertica backend</li>
        <li>create mappings for above campaign names and save it as a <b>CSV file</b></li>
        <li>upload that updated mapping CSV file using 'InCampaign_Facebook_Campaign_Group_Name_Mapping' feed in DataVault</li>
        <li>run a similar SQL in Vertica backend to truncate all tables listed in the subject title:
            <b>TRUNCATE TABLE gaintheory_us_targetusa_14.[table_name];</b></li>
        <li>run this SQL in Vertica backend: <br>
            <b>
            UPDATE gaintheory_us_targetusa_14.incampaign_process_switches
            SET run = 1
            WHERE process_name = '{1}';
            </b>
        </li>
        </ol>
        <p><strong style="color: red;">Once you have truncated the mapping table, please reset the flag,
        <b>{1}</b>, and we'll try rerunning the data processing automatically.</strong></p>
        """.format(table, flag)


def main():
    flag_name = 'fb_etl_and_export_to_s3'
    try:
        with vertica_python.connect(**conn_info) as connection:
            tables_to_truncate = ['incampaign_facebook_campaign_group_names_to_map']
            flag_name_and_actions = {
                flag_name:
                    [
                        {
                            'pre_cmd': {'tables_to_truncate': tables_to_truncate,
                                        'reminder': {
                                            'subject': "Please truncate table(s): " + str(tables_to_truncate),
                                            'body': remind_to_truncate_table(str(tables_to_truncate), flag_name),
                                            'recipients': NOTIFICATION_EMAIL_RECIPIENTS
                                        }},
                            'cmd': ['python', ROOT_FOLDER + 'run_vsql.py', SQL_SCRIPT_FOLDER + 'fb_etl.sql']
                        },
                        # {'cmd': ['python', ROOT_FOLDER + 'archive_files.py', 'FilesForDatamart/Facebook/',
                        #          S3_ARCHIVE_ROOT + 'Facebook/']}, # NOTE: we'll not archive fb files because they're too big and could delay the processing time
                        {'cmd': ['python', ROOT_FOLDER + 'run_vsql_and_export_to_s3.py',
                                 SQL_SCRIPT_FOLDER + 'export_fb.sql',
                                 'Facebook/', 'FilesForDatamart/Facebook/', 'fb']}
                    ]
            }
            trigger_on_flag_value_change(flag_name_and_actions)
    except:
        raise

if __name__ == "__main__":
    main()
