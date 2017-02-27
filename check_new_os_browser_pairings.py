"""
Python script to keep checking if a file appears in specified S3 folders.
"""
import os
import time
import csv
import schedule


from mailer import Mailer
from logger import Logger
from s3_utils import *
from vertica_utils import *


def notify_for_manual_mapping(file):
    email_str = """
        <p>Python script extracted new os-browser pairings from Vault DCM data.</p>
        <p>To run the DCM Mapping extraction process correctly, please do the followings:
        <ol>
        <li>download the attached file, <b>{0}</b>, from this email</li>
        <li>fill up empty mappings under column C ("device") in that file</b></li>
        <li>upload the modified file via this feed in DataVault: <b>InCampaign_DCM_OS_And_Browser_To_Device_Mappings</b></li>
        </ol>
        </p>
        <p> Once the device mapping is uploaded as instructed above, the rest of the automated process will
        proceed automatically.</p>
        """.format(file)
    return email_str


def send_error_email(error):
    subject = "ERROR in checking new os-browser pairings"
    body = """
    <p>While checking new os-browser pairings in Vault's DCM tables, we ran into an error below:</p>
    <p><strong style="color: red;">{0}</strong></p>
    """.format(str(error))
    Mailer().send_email(ERROR_EMAIL_RECIPIENTS, subject, body)
    print("Error notification email sent")


def get_unmapped_os_browser_pairings():
    return """
        SELECT DISTINCT
            operating_system,
            browser_platform
        FROM
            gaintheory_us_targetusa_14.incampaign_dcm_mapping_reference_from_vault
        EXCEPT
        SELECT
            operating_system,
            browser_platform
        FROM
            gaintheory_us_targetusa_14.incampaign_dcm_os_browser_to_device_mappings
    """


def get_existing_os_browser_device_mappings():
    return """
    SELECT
        operating_system,
        browser_platform,
        device
        FROM
            gaintheory_us_targetusa_14.incampaign_dcm_os_browser_to_device_mappings
    """

def main():
    logger = Logger(__file__)
    start_time = time.ctime()

    # Location of sources and destination files
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'DCMMapping')
    output_file = 'new_dcm_mappings.csv'
    file_to_export = os.path.join(output_folder, output_file)

    if not os.path.exists(output_folder):
        print("Creating a new local folder for export file:", output_folder)
        os.makedirs(output_folder)

    try:
        with vertica_python.connect(**conn_info) as vertica_conn:
            print("Checking new browser os pairings...")
            cursor = vertica_conn.cursor()

            new_pairings = get_rows(cursor, get_unmapped_os_browser_pairings())
            if not new_pairings:
                print("No new pairings found today")
            else:
                headers = [['operating_system','browser_platform','device']]
                existing_pairings = get_rows(cursor, get_existing_os_browser_device_mappings())
                all_pairings = headers + [[x[0], x[1], ''] for x in new_pairings] + existing_pairings
                with open(file_to_export, 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=',')
                    csvwriter.writerows(all_pairings)

        print("Found new browser-os pairings and sending them to analysts")
        # Send email to tell the team to start manual mapping
        subject = "New Device mappings to fill out for DCM os-browser pairings:"
        body = notify_for_manual_mapping(output_file)
        Mailer().send_email(DEV_EMAIL_RECIPIENTS, subject, body, file_to_export) #TODO: change email receipients
        print("Notified the team to add manual mappings")

        logger.log_time_taken(start_time, time.ctime())
        print("Finished checking new browser-os pairings in Vault DCM tables...")
    except vertica_python.errors.QueryError as err:
        print("Vertica Query Error!")
        send_error_email(err)
    except ConnectionError as err:
        print("Connection error: ", err)
    except Exception as err:
        print("Unknown Error Occurred!")
        send_error_email(err)

if __name__ == "__main__":
    print("\n\n*****DO NOT KILL this program*****\n")
    print("If you accidentally or intentionally killed this program, please rerun it")
    print("This program runs processes everyday at 11AM EST")

    schedule.every().friday.at("12:00").do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)
