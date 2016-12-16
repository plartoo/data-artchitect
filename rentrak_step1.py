"""
TODO: update descriptino after everything is concrete
Description: Script to extract KeepingTrac's creative names and send
team notification to start manual mapping as necessary.

This step must happen BEFORE the processing of deduping of RenTrak
creative names (step 2 in RenTrak processing).
"""
import pandas as pd

from mailer import Mailer
from vertica_utils import *
from s3_utils import *


def notify_for_manual_mapping(file, process_name):
    email_str = """
        <p>Python script extracted new creative names from KeepingTrac data.</p>
        <p>To run the rest of the RenTrak ETL process smoothly, please do the followings:
        <ol>
        <li>download the attached file, <b>{0}</b>, from this email</li>
        <li>fill up empty (nan/NULL) kt_creative mappings under column C (kt_creative_clean) in that file</b></li>
        <li>upload the modified file to the S3 location below
        <span style="color: red;">(replace any file with the same name in the S3 folder, if any)</span>:<br>
        <b>diap.prod.us-east-1.target/RenTrak/CreativeCleaned</b>
        </li>
        <li>run this feed in DataVault: <b>InCampaign KT Creative Mappings</b></li>
        <li><span style="color: red;">AFTER the DataVault feed successfully loaded the mappings</span>,
            run this SQL in Vertica backend: <br>
            <b>
            UPDATE gaintheory_us_targetusa_14.incampaign_process_switches
            SET run = 1
            WHERE process_name = '{1}';
            </b>
        </li>
        </ol>
        </p>
        <p><strong style="color: red;">NOTE: If you forget a step or more above, the second part of RenTrak processing
        may not produce correct results.</strong></p>
        """.format(file, process_name)
    return email_str


def notify_no_new_mapping_found():
    email_str = """
        <p>Python script does not find any new creative names from keepingtrac data.
        Stage 2 of processing RenTrak data will begin when we load new data to RenTrak tables.
        </p>
        <p><b>No further action on your part is needed.</b></p>
        """
    return email_str


def send_notification_email(recipients, subject, body, attachment=None):
    Mailer().send_email(recipients, subject, body, attachment)
    print("Notification email sent.")


# Function to extract data from vertica into a pandas dataframe
def vertica_extract(query, columns, index=None):
    with vertica_python.connect(**conn_info) as connection:
        cur = connection.cursor()
        cur.execute(query)
        results = pd.DataFrame(cur.fetchall())
        results.columns = columns
        if index:
            return results.set_index(index)
        else:
            return results


def set_flag_value(table_name, schema_name, flag_name, value):
    return """
    UPDATE {1}.{0}
    SET run = {3}
    WHERE process_name = '{2}';
    COMMIT;
    """.format(table_name, schema_name, flag_name, value)


def set_lock(table_name, schema_name, flag_name, value):
    with vertica_python.connect(**conn_info) as connection:
        cur = connection.cursor()
        cur.execute(set_flag_value(table_name, schema_name, flag_name, value))
        connection.commit()


def main():
    # start_date = (today - datetime.timedelta(weeks=6, days=1)).strftime('%Y-%m-%d')
    schema_name = 'gaintheory_us_targetusa_14'
    mapping_table = 'incampaign_kt_creative_mappings'
    flag_table = 'incampaign_process_switches'
    flag = 'rentrak_kt_creative_cleaned'

    # Location of sources and destination files
    output_folder = ROOT_FOLDER + 'RenTrak'
    output_file = 'kt_creative_cleaned.xlsx'

    if not os.path.exists(output_folder):
        print("Creating a new local folder for export file:", output_folder)
        os.makedirs(output_folder)

    # Step 1: Download all possible KT combinations and current matching cleaned creative names
    extract_query = """
        SELECT Air_ISCI as kt_creative_id, Cmml_Title AS kt_creative, kt_creative_clean
        FROM {1}.keepingtrac a
        LEFT JOIN {1}.{0} b
        ON a.Air_ISCI = b.kt_creative_id
        WHERE Air_ISCI IS NOT NULL
        GROUP BY a.Air_ISCI, a.Cmml_Title, kt_creative_clean
        ORDER BY kt_creative_id
    """.format(mapping_table, schema_name)

    df = vertica_extract(
        extract_query,
        ['kt_creative_id', 'kt_creative', 'kt_creative_clean']
    )
    df = df.dropna(how='any') # drop blank rows
    unmapped_creatives = sum(x == 'nan' for x in df['kt_creative_clean'])

    if unmapped_creatives > 0:
        print("Some unmapped kt_creatives found")
        print("Acquiring process lock:", flag, "so that the second part of RenTrak processing cannot proceed")
        set_lock(flag_table, schema_name, flag, 0)

        file_to_export = os.path.join(output_folder, output_file)
        df.to_excel(file_to_export, index=False)

        # Send email to tell the team to start manual mapping
        subject = "RenTrak automated processing step 1: new kt_creatives need to be mapped"
        body = notify_for_manual_mapping(output_file, flag)
        send_notification_email(ONSHORE_EMAIL_RECIPIENTS, subject, body, file_to_export)
        print("Notified the team to add manual mapping")

        os.remove(file_to_export)
        print("Deleted local file=>", file_to_export)

    else:
        print("Everything is mapped")
        print("Releasing process lock:", flag, "so that the second part of RenTrak processing can proceed")
        set_lock(flag_table, schema_name, flag, 1)

        # insert, set flag to 1 and send email notification about being cleaned
        subject = "RenTrak automated processing step 1: kt_creatives are all mapped. Step 2 will automatically commence."
        body = notify_no_new_mapping_found()
        send_notification_email(ONSHORE_EMAIL_RECIPIENTS, subject, body)
        print("Notified the team that no further action on their part is required")

if __name__ == "__main__":
    main()
