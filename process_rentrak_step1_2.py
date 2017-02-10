import time
import schedule

from trigger_on_row_count_change import *
from logger import Logger


def main():
    table_and_actions = {
        'incampaign_keepingtrac_all':
            [
                {'cmd': ['python', ROOT_FOLDER + 'rentrak_step1.py']},
            ],
        'incampaign_rentrak_zipcode': # we will watch zipcode table because its loads more data and is slower to complete
            [
                {'cmd': ['python', ROOT_FOLDER + 'rentrak_step2.py']},
            ],
    }

    logger = Logger(__file__)
    start_time = time.ctime()
    trigger_on_row_count_change(table_and_actions, 3)
    logger.log_time_taken(start_time, time.ctime())


if __name__ == "__main__":
    interval = 30
    print("\n\n*****DO NOT KILL this program*****\n")
    print("If you accidentally or intentionally killed this program, please rerun it")
    print("This program runs processes every:", interval, "minutes(s)")

    # we'll have at least 1.5 hours delay to wait for Vault data table loading to complete
    schedule.every(interval).minutes.do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)
