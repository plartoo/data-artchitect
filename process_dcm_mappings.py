import time
import schedule

from trigger_once import *
from logger import Logger


def get_msg_body_for_completion(table_name):
    return """
        <p>Python script completed mappings for DCM (digital) data.</p>
        <p>The result table is: {0}</p>
        """.format(table_name)


def main():
    table_name = 'incampaign_digital_metadata'
    actions = [
                {'cmd': ['python', ROOT_FOLDER+'run_vsql.py', SQL_SCRIPT_FOLDER + 'create_mappings_using_dcm_and_prisma.sql'],
                 'notify_on_complete': {
                     'subject': 'DCM Mappings completed',
                     'body': get_msg_body_for_completion(table_name),
                     'recipients': ONSHORE_EMAIL_RECIPIENTS + OFFSHORE_EMAIL_RECIPIENTS}
                 }
            ]

    logger = Logger(__file__)
    start_time = time.ctime()
    trigger_once(actions)
    logger.log_time_taken(start_time, time.ctime())


if __name__ == "__main__":
    print("\n\n*****DO NOT KILL this program*****\n")
    print("If you accidentally or intentionally killed this program, please rerun it")
    print("This program runs processes every: Thursday at 1pm EST")

    main()
    #schedule.every().friday.at("14:39").do(main)
    #
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
