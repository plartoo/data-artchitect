import time
import schedule

from trigger_once import *
from account_info import *
from vertica_utils import *
from logger import Logger


def get_msg_body_for_completion(schema_name, tables_to_check):
    msg_body = """
        <p>Vault2Mart automation process has created an IRT file out of the following tables:</p>
        <ul>
        """

    with vertica_python.connect(**conn_info) as connection:
        cursor = connection.cursor()

        for table_name in tables_to_check:
            max_date = get_date_range(cursor, table_name, schema_name, 'Period')
            min_date = get_date_range(cursor, table_name, schema_name, 'Period', min_or_max='min')
            msg_body += ''.join(['<li><b>' + table_name + '</b>=> ', str(min_date[0]), ' - ', str(max_date[0])])

    msg_body += """
        </ul>
        <p>
        <strong style="color: red;"> Please check the date range listed above to make sure they are OK for your
        weekly analysis.</strong> If something is amiss, please contact the onshore data architect to
        investigate what could possibly have gone wrong.
        </p>
        """
    return msg_body


def main():
    schema = 'gaintheory_us_targetusa_14'
    tables_to_check = ['incampaign_dfa_clicks_last_60_days', 'incampaign_dfa_impressions_last_60_days',
                       'incampaign_facebook_last_60_days', 'incampaign_keepingtrac_local_last_60_days',
                       'incampaign_rentrak_last_60_days', 'incampaign_storesales_last_60_days',
                       'incampaign_websales_last_60_days'
                       ]
    actions = [
        {
            'cmd': ['python', ROOT_FOLDER+'run_vsql.py', SQL_SCRIPT_FOLDER+'create_irt_table.sql'],
            'notify_on_complete': {
                 'subject': 'Incampaign IRT table is created: please make sure to check the date ranges in this email for different data sources',
                 'body': get_msg_body_for_completion(schema, tables_to_check),
                 'recipients': ONSHORE_EMAIL_RECIPIENTS}
         }
    ]

    logger = Logger(__file__)
    start_time = time.ctime()
    trigger_once(actions)
    logger.log_time_taken(start_time, time.ctime())


if __name__ == "__main__":
    # every Monday at 8:30pm (agreed on this time with Manoj on Feb 13, 2017)
    print("\n\n*****DO NOT KILL this program*****\n")
    print("If you accidentally or intentionally killed this program, please rerun it")
    print("This program runs processes every: Monday at 4pm EST")

    schedule.every().monday.at("20:30").do(main)

    while True:
        schedule.run_pending()
        time.sleep(30)
