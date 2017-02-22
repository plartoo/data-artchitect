import time
import schedule

from trigger_on_row_count_change import *
from logger import Logger


def get_msg_body_for_completion(s3_export_folder, data_source_name):
    return """
        <p>Python script extracted the data from Vault and exported to this S3 location: <b>{0}</b></p>
        <p>To the offshore team, please make sure that the latest file in the above S3 folder
        (marked with the timestamp, which has this email's date) is processed via this Data Source: <br>
        <b>{1}</b>
        <br>
        <strong style="color: red;">up to the 'Transformed' step (that is, all missing values added and 'Transformed'
        status should be green)</strong></p>
        """.format(s3_export_folder, data_source_name)


def main():
    s3_folder = 'FilesForDatamart/StoreSales/'
    data_source_name = 'InCampaign Store Sales (DM 3128)'
    table_and_actions = {
        'incampaign_storesales':
            [
                {'cmd': ['python', ROOT_FOLDER + 'run_vsql.py', SQL_SCRIPT_FOLDER + 'process_storesales.sql']},
                # {'cmd': ['python', ROOT_FOLDER + 'archive_files.py', 'FilesForDatamart/StoreSales/',
                #          S3_ARCHIVE_ROOT + 'StoreSales/']},
                {'cmd': ['python', ROOT_FOLDER + 'run_vsql_and_export_to_s3.py',
                         SQL_SCRIPT_FOLDER + 'export_storesales.sql', 'StoreSales/', s3_folder, 'storesales'],
                'notify_on_complete': {
                    'subject': 'Incampaign Storesales processed: please make sure that the Datamart completes the ETL process',
                    'body': get_msg_body_for_completion(s3_folder, data_source_name),
                    'recipients': ONSHORE_EMAIL_RECIPIENTS + OFFSHORE_EMAIL_RECIPIENTS}
                }
            ]
    }

    logger = Logger(__file__)
    start_time = time.ctime()
    trigger_on_row_count_change(table_and_actions, 2)
    logger.log_time_taken(start_time, time.ctime())


if __name__ == "__main__":
    interval = 1
    print("\n\n*****DO NOT KILL this program*****\n")
    print("If you accidentally or intentionally killed this program, please rerun it")
    print("This program runs processes every:", interval, "hour(s)")

    schedule.every(interval).hours.do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)
