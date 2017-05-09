import os
import time
import schedule

from trigger_on_row_count_change import *
from logger import Logger


def main():
    table_and_actions = {
        'v_incampaign_facebook_impressions_and_spend':
            [
                {'cmd': ['python', ROOT_FOLDER+'run_vsql.py', SQL_SCRIPT_FOLDER+'fb_extract_gp_names.sql']},
                {'cmd': ['python', ROOT_FOLDER + 'fb_step1_post_process.py']}
            ]
    }

    logger = Logger(__file__)
    start_time = time.ctime()
    trigger_on_row_count_change(table_and_actions, 1)
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
