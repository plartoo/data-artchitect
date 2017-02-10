"""
Python script to keep checking if a file appears in specified S3 folders.
"""
import time
import schedule

from mailer import Mailer
from logger import Logger
from s3_utils import *
from vertica_utils import *


def send_notification_email(folder_name, file_names, feed_name):
    subject = "New file found in S3 folder: " + folder_name
    body = """
    <p>The following file(s) <br><strong style="color: red;">{1}</strong><br> is(are) found in S3 folder named:
    <br><b>{0}</b><br></p>
    <p>Please load it(them) to DataVault feed, <b>{2}</b>, or take some appropriate action.</p><br><br>
    <p>If you think this email should no longer be sent (to you), please let Phyo know.</p>
    """.format(folder_name, file_names, feed_name)
    Mailer().send_email(ONSHORE_EMAIL_RECIPIENTS, subject, body)
    print("New file found email successfully sent.")


def send_error_email(error_msg):
    subject = "ERROR in checking new files in S3"
    body = """
    <p>While checking new files in S3, we ran into error below:</p>
    <p><strong style="color: red;">{0}</strong></p>
    """.format(error_msg)
    Mailer().send_email(ERROR_EMAIL_RECIPIENTS, subject, body)
    print("Error notification email sent")


def create_record_table(table_name, schema_name):
    return """
    CREATE TABLE
        IF NOT EXISTS {1}.{0}
        (
            folder_name VARCHAR(1000),
            file_name  VARCHAR(1000),
            seen_date DATE
        );
    """.format(table_name, schema_name)


def record_table_name(table_name, schema_name, folder, file):
    return """
    INSERT INTO {1}.{0} VALUES ('{2}', '{3}', GETDATE()); COMMIT;
    """.format(table_name, schema_name, folder, file)


def file_exists_in_record(cursor, file_name, folder, table_name, schema_name):
    filter = (' WHERE file_name=\'' + file_name + '\' AND folder_name=\'' + folder +'\';')
    return (get_row_count_by_filter(cursor, filter, table_name, schema_name)[0] > 0)


def get_single_quoted_str(list_of_str):
    return ','.join(['\'{}\''.format(i) for i in list_of_str])


def main():
    logger = Logger(__file__)
    start_time = time.ctime()
    file_seen_table = 'incampaign_file_seen'
    schema_name = 'gaintheory_us_targetusa_14'

    # Note: make sure to configure auth credentials as shown here: https://boto3.readthedocs.io/en/latest/guide/quickstart.html
    s3_folders_and_feeds = {'Facebook/': 'InCampaign_Facebook_Impressions_And_Spend',
                           'RenTrak/SpotID/': 'Incampaign_Rentrak_SpotID',
                           'RenTrak/ZipCode/': 'Incampaign_Rentrak_ZipCode',
                           'TargetInbound/StoreSales/': 'InCampaign_StoreSales_Zipcode',
                           'TargetInbound/WebSales/': 'InCampaign_WebSales_Zipcode',
                           'TargetInbound/MondayStoreSales/': 'InCampaign_StoreSales_Zipcode_Monday',
                            'TargetInbound/MondayWebSales/': 'InCampaign_WebSales_Zipcode_Monday'
                            }

    try:
        with vertica_python.connect(**conn_info) as vertica_conn:
            print("Checking for new files in S3...")
            cursor = vertica_conn.cursor()
            cursor.execute(create_record_table(file_seen_table, schema_name))

            for folder, feed in s3_folders_and_feeds.items():
                new_files = []
                for f_name in list_key_names(folder):
                    if not file_exists_in_record(cursor, f_name, folder, file_seen_table, schema_name):
                        print("new file found:", f_name)
                        new_files.append(f_name)
                        cursor.execute(record_table_name(file_seen_table, schema_name, folder, f_name))

                if len(new_files) > 0:
                    send_notification_email(folder, '<br>'.join(new_files), feed)

        logger.log_time_taken(start_time, time.ctime())
        print("Finished checking new files in S3...")
    except vertica_python.errors.QueryError as err:
        print("Vertica Query Error!")
        send_error_email(err)
    except ConnectionError as err:
        print("Connection error: ", err)
    except Exception as err:
        print("Unknown Error Occurred!")
        send_error_email(err)

if __name__ == "__main__":
    interval = 15
    print("\n\n*****DO NOT KILL this program*****\n")
    print("If you accidentally or intentionally killed this program, please rerun it")
    print("This program runs processes every:", interval, "minute(s)")

    schedule.every(interval).minutes.do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)
