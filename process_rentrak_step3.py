import os
import time
import schedule

from trigger_on_flag_value_change import *
from logger import Logger


def get_msg_body_for_completion(s3_export_folder, data_source_name):
    return """
        <p>Python script extracted the RenTrak data from Vault and exported to this S3 location: <b>{0}</b></p>
        <p>To the offshore team, please verify that the processing of the following Data Source: <br>
        <b>{1}</b>
        <br>
        is <b>finished</b> before the start of Tuesday morning (9am) in India. Note that the Data Source is scheduled
        to automatically start running by 1AM India time on Tuesday morning, and it should normally take no more than
        3 hours to finish the ETL).
        <br>
        <strong style="color: red;"> If the Data Source automatic ETL failed, please notify onshore team and
        do anything is necessary to ETL it to completion ASAP.</strong>
        </p>
        """.format(s3_export_folder, data_source_name)


def main():
    s3_folder = 'FilesForDatamart/RenTrak/'
    data_source_name = 'InCampaign RenTrak (DM 3128)'
    cmds_to_run = [
                {
                    'cmd': ['python', ROOT_FOLDER + 'run_vsql.py', SQL_SCRIPT_FOLDER + 'rentrak_etl.sql']
                },
                {'cmd': ['python', ROOT_FOLDER + 'run_vsql_and_export_to_s3.py',
                         SQL_SCRIPT_FOLDER + 'export_rentrak.sql',
                         'RenTrak/', s3_folder, 'rentrak'],
                 'notify_on_complete': {
                     'subject': 'Incampaign RenTrak data processed: please make sure that the Datamart completes the ETL process',
                     'body': get_msg_body_for_completion(s3_folder, data_source_name),
                     'recipients': ONSHORE_EMAIL_RECIPIENTS + OFFSHORE_EMAIL_RECIPIENTS}
                 }
            ]

    flag_name_and_actions = {
        # On May 9, 2017, Manoj and I agree to only rely on either this flag below
        # or rentrak_zipcode table row count change (therefore, the rentrak_creative_match_dedupe will happen first)
        # to trigger the rest of RenTrak processing

        'rentrak_creative_match_deduped': cmds_to_run,
        'rentrak_kt_creative_cleaned': cmds_to_run
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
