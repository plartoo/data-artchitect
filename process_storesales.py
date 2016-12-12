from trigger_on_row_count_change import *


def get_msg_body_for_completion(s3_export_folder, data_source_name):
    return """
        <p>Python script extracted the data from Vault and exported to this S3 location:<b>{0}</b></p>
        <p>To the offshore team, please make sure that the latest file in that folder
        (marked with the timestamp of this email's date) is processed by this Data Source: <b>{1}</b>
        <strong style="color: red;">up to the 'Transformed' step (all missing values added and green)</strong></p>
        """.format(s3_export_folder, data_source_name)


def main():
    s3_folder = 'FilesForDatamart/StoreSales/'
    data_source_name = 'InCampaign Store Sales'
    table_and_actions = {
        'incampaign_storesales_zipcode':
            [
                {'cmd': ['python', ROOT_FOLDER + 'run_vsql.py', SQL_SCRIPT_FOLDER + 'process_storesales.sql']},
                # {'cmd': ['python', ROOT_FOLDER + 'archive_files.py', 'FilesForDatamart/StoreSales/',
                #          S3_ARCHIVE_ROOT + 'StoreSales/']},
                {'cmd': ['python', ROOT_FOLDER + 'run_vsql_and_export_to_s3.py',
                         SQL_SCRIPT_FOLDER + 'export_storesales.sql', 'StoreSales/', s3_folder, 'storesales'],
                'notify_on_complete': {
                    'subject': 'Incampaign Storesales processed: please make sure that the Datamart completes ETL process',
                    'body': get_msg_body_for_completion(s3_folder, data_source_name),
                    'recipients': NOTIFICATION_EMAIL_RECIPIENTS} # TODO: Add offshore email OFFSHORE_EMAIL_RECIPIENTS
                }
            ]
    }
    trigger_on_row_count_change(table_and_actions)

if __name__ == "__main__":
    main()
