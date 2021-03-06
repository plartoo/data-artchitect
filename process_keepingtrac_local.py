import os
import time
import schedule

from trigger_once import *
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
        Note: This Data Source is scheduled to automatically ETL on Monday at 4pm (EST), but if automated ETL failed,
        please do anything necessary to finish the ETL BEFORE Tuesday morning (9am) in India.
        </strong></p>
        """.format(s3_export_folder, data_source_name)


def main():
    s3_folder = 'FilesForDatamart/KeepingTracLocal/'
    data_source_name = 'InCampaign KeepingTrac Local (DM 3128)'
    actions = [
                {'cmd': ['python', ROOT_FOLDER+'run_vsql.py', SQL_SCRIPT_FOLDER+'keepingtrac_local_etl.sql']},
                {'cmd': ['python', ROOT_FOLDER + 'run_vsql_and_export_to_s3.py', SQL_SCRIPT_FOLDER + 'export_keepingtrac_local.sql',
                         'KeepingTracLocal/', s3_folder, 'kt'],
                 'notify_on_complete': {
                     'subject': 'Incampaign KeepingTrac Local data processed: please make sure that the Datamart completes the ETL process',
                     'body': get_msg_body_for_completion(s3_folder, data_source_name),
                     'recipients': ONSHORE_EMAIL_RECIPIENTS + OFFSHORE_EMAIL_RECIPIENTS}
                 }
            ]

    logger = Logger(__file__)
    start_time = time.ctime()
    trigger_once(actions)
    logger.log_time_taken(start_time, time.ctime())


if __name__ == "__main__":
    # every Monday at 3pm, KeepingTrac local is loaded
    print("\n\n*****DO NOT KILL this program::", os.path.basename(__file__) ,"*****\n")
    print("If you accidentally or intentionally killed this program, please rerun it")
    print("This program runs processes every: Monday at 4pm EST")

    schedule.every().monday.at("16:00").do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)
