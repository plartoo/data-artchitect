import os
import time
import sys
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..'))

import schedule
from logger import Logger


def main():
    print("This is bob at: ", time.ctime())

if __name__ == "__main__":
    interval = 2
    dir_name = os.path.dirname(os.path.abspath(__file__))
    file_with_path = os.path.join(dir_name, __file__)
    logger = Logger(__file__)
    print("\n\n*****DO NOT KILL this program*****\n")
    print("If you accidentally or intentionally killed this program, please rerun it")
    print("This program runs processes every:", interval, "secs")

    log_msg = "\n\n" + file_with_path + "\n"
    log_msg += "This program runs every: " + str(interval) + "secs\n"
    log_msg += "It was invoked at: " + time.strftime("%c")

    schedule.every(1).minutes.do(main)
    while True:
        schedule.run_pending()
        # logger.log(log_msg)
        print("right after run_pending")
        time.sleep(interval)

