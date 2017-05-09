import os
import time
import schedule

from trigger_on_flag_value_change import *
from logger import Logger


def get_msg_body_for_completion(s3_export_folder, data_source_name):
    return """
        <p>Python script extracted the data from Vault and exported to this S3 location: <b>{0}</b></p>
        <p>To the offshore team, please make sure that the latest file in the above S3 folder
        (marked with the timestamp, which has this email's date) is ETL-ed via this Data Source: <br>
        <b>{1}</b>
        <br>
        <strong style="color: red;">
        <br>
        Note: This Data Source is scheduled to automatically ETL on every weekend, but if the weekend ETL failed, please
        do anything necessary to finish the ETL BEFORE Tuesday morning (9am) in India.
        </strong></p>
        """.format(s3_export_folder, data_source_name)


def main():
    s3_folder = 'FilesForDatamart/Facebook/'
    data_source_name = 'InCampaign Facebook (DM 3128)'
    flag_name_and_actions = {
        'fb_etl_and_export_to_s3':
            [
                {
                    'cmd': ['python', ROOT_FOLDER + 'run_vsql.py', SQL_SCRIPT_FOLDER + 'fb_etl.sql']
                },
                # {'cmd': ['python', ROOT_FOLDER + 'archive_files.py', 'FilesForDatamart/Facebook/',
                #          S3_ARCHIVE_ROOT + 'Facebook/']}, # NOTE: we'll not archive fb files because they're too big and could delay the processing time
                {'cmd': ['python', ROOT_FOLDER + 'run_vsql_and_export_to_s3.py', SQL_SCRIPT_FOLDER + 'export_fb.sql',
                         'Facebook/', s3_folder, 'fb'],
                 'notify_on_complete': {
                     'subject': 'Incampaign Facebook data processed: please make sure that the Datamart completes the ETL process',
                     'body': get_msg_body_for_completion(s3_folder, data_source_name),
                     'recipients': ONSHORE_EMAIL_RECIPIENTS + OFFSHORE_EMAIL_RECIPIENTS}
                 }
            ]
    }

    logger = Logger(__file__)
    start_time = time.ctime()
    trigger_on_flag_value_change(flag_name_and_actions)
    logger.log_time_taken(start_time, time.ctime())


if __name__ == "__main__":
    interval = 1
    print("\n\n*****DO NOT KILL this program::", os.path.basename(__file__) ,"*****\n")
    print("If you accidentally or intentionally killed this program, please rerun it")
    print("This program runs processes every:", interval, "hour(s)")

    schedule.every(interval).hours.do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)
