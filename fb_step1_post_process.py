"""
TODO: Refactor this to merge with rentrak_step1.py
"""
import pandas as pd
from pandas import ExcelWriter

from mailer import Mailer
from vertica_utils import *
from s3_utils import *


def notify_for_manual_mapping(file, process_name):
    return """
        <p>Python script just extracted campaign group names from new fb impression and spend loaded to Vault recently.</p>
        <p>To run the rest of the ETL process, please do the followings:
        <ol>
        <li>download the attached file in this email: <b>{0}</b></li>
        <li>create mappings for campaign names (listed under column A) in column B
        (refer to the sheet named 'keys' for mapping names that you are allowed to use)</li>
        <li>upload the modified file to the S3 location below
        <span style="color: red;">(replace any file with the same name in the S3 folder, if any)</span>:<br>
        <b>diap.prod.us-east-1.target/FacebookMappings</b>
        </li>
        <li>run this feed in DataVault: <b>InCampaign_Facebook_Campaign_Group_Name_Mapping</b></li>
        <li><span style="color: red;">AFTER the DataVault feed loaded the mappings successfully</span>,
            run this SQL in Vertica backend: <br>
            <b>
            UPDATE gaintheory_us_targetusa_14.incampaign_process_switches
            SET run = 1
            WHERE process_name = '{1}';
            </b>
        </li>
        </ol>
        </p><br>
        <p><strong style="color: red;">NOTE: If you fail to run UPDATE SQL query as directed above, the second
        part of facebook data will not be processed.</strong></p>
        """.format(file, process_name)


def notify_no_new_mapping_found():
    email_str = """
        <p>Python script does not find any new creative names from new fb impression and spend data.
        Stage 2 of processing Facebook data will automatically begin in a moment and you'll receive
        another notification message when everything is finished.</p>
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

        if not results.empty:
            results.columns = columns
            if index:
                return results.set_index(index)

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


def main():
    schema_name = 'gaintheory_us_targetusa_14'
    new_mapping_table = 'incampaign_facebook_campaign_names_to_map'
    existing_mapping_table = 'incampaign_facebook_mapped_campaign_group_name'
    flag_table = 'incampaign_process_switches'
    flag = 'fb_etl_and_export_to_s3'
    columns_to_extract = ['fb_campaign_group_name']

    # Location of sources and destination files
    output_folder = ROOT_FOLDER + 'Facebook'
    output_file = 'campaign_gp_name_mappings.xlsx'

    if not os.path.exists(output_folder):
        print("Creating a new local folder for output file:", output_folder)
        os.makedirs(output_folder)

    extract_query = """
        SELECT *
        FROM {1}.{0}
        ORDER BY 1
    """.format(new_mapping_table, schema_name)
    mappings = vertica_extract(extract_query, columns_to_extract)

    get_unique_gp_names = """
        SELECT DISTINCT fb_mapped_campaign_group_name
        FROM {1}.{0}
        ORDER BY 1
    """.format(existing_mapping_table, schema_name)
    keys = vertica_extract(get_unique_gp_names, ['fb_mapped_campaign_group_name'])

    if not mappings.empty:
        print("Some unmapped campaign group names found")
        print("Acquiring process lock:", flag, "so that the second part of Facebook processing cannot proceed")
        set_lock(flag_table, schema_name, flag, 0)

        mappings = mappings.dropna(how='any')  # drop blank rows; needs to happen after 'if not empty' check

        # Write data to excel file
        file_to_export = os.path.join(output_folder, output_file)
        writer = ExcelWriter(file_to_export)
        mappings.to_excel(writer, sheet_name='mappings', index=False)
        keys.to_excel(writer, sheet_name='keys', index=False, header=False)
        writer.save()

        # Truncate the new mapping table
        with vertica_python.connect(**conn_info) as connection:
            cur = connection.cursor()
            truncate_table(cur, new_mapping_table, schema_name)

        # Send email to tell the team about the need for manual mapping
        subject = "Facebook automated processing step 1: new campaign group names need to be mapped"
        body = notify_for_manual_mapping(output_file, flag)
        send_notification_email(ONSHORE_EMAIL_RECIPIENTS, subject, body, file_to_export)
        print("Notified the team to add manual mapping")

        # Delete local file
        os.remove(file_to_export)
        print("Deleted local file=>", file_to_export)
    else:
        print("Everything is mapped")
        print("Releasing process lock:", flag, "so that the second part of Facebook processing can proceed")
        set_lock(flag_table, schema_name, flag, 1)

        # insert, set flag to 1 and send email notification about being cleaned
        subject = "Facebook automated processing step 1: campaign group names are all mapped. Step 2 will automatically commence."
        body = notify_no_new_mapping_found()
        send_notification_email(ONSHORE_EMAIL_RECIPIENTS, subject, body)
        print("Notified the team that no further action on their part is required")

if __name__ == "__main__":
    main()
