import datetime
import pandas as pd

from mailer import Mailer
from vertica_utils import *
from s3_utils import *

def notify_for_manual_mapping(file, table, process):
    email_str = """
        <p>Our python script extracted new creative names from keepingtrac data.</p>
        <p>To run the rest of the RenTrak ETL process smoothly, please do the followings:
        <ol>
        <li>download <b>{0}</b> file from S3 location: <b>diap.prod.us-east-1.target/RenTrak/CreativeCleaned</b></li>
        <li>fill up mapings for kt_creative in column C (kt_creative_clean) of that file</b></li>
        <li>upload that data back to S3 location above

        (use Python script provided at the end of this email or DB Visualizer)</li>
        <li>run this SQL in Vertica backend: <br>
            <b>
            UPDATE gaintheory_us_targetusa_14.incampaign_process_switches
            SET run = 1
            WHERE process_name = '{2}';
            </b>
        </li>
        </ol>
        </p>
        <p><strong style="color: red;">NOTE: If you fail to do as directed above, the second part of RenTrak processing
        may not produce correct results.</strong></p>
        <p> You can use Python code (reformat and fill out necessary variable values to run successfully)
        similar to example shown below to upload the mapping file back to the Vertica table: </p>
        <p>
        <code>
            # Vertica access details<br>
            vertica_user = # enter vertica user name here<br>
            vertica_pass = # enter vertica password here<br>
            data_folder = # enter the name of the folder where you saved the mapping file; e.g., 'C:/Users/Totoro/Desktop/RenTrakMapping'<br>
            kt_cleaned = # enter name of file on your computer that you updated mapping for; e.g., kt_cleaned_2016-11-01_2016-09-01.xlsx<br>
            <br>
            # Vertica connection string<br>
            conn_info = # enter vertica connection information like server address, port number, etc.<br>
            <br>
            creation_query = \"\"\"<br>
                DROP TABLE IF EXISTS gaintheory_us_targetusa_14.{1};<br>
                CREATE TABLE gaintheory_us_targetusa_14.{1} (<br>
                        kt_creative_id   varchar(150),<br>
                        kt_creative_clean varchar(1500)<br>
                );<br>
            \"\"\"<br>
            <br>
            insert_query = \"\"\"<br>
                INSERT INTO gaintheory_us_targetusa_14.{1}(kt_creative_id, kt_creative_clean) VALUES ('%s', '%s')<br>
            \"\"\"<br>
            <br>
            with vertica_python.connect(**conn_info) as connection:<br>
                cur = connection.cursor()<br>
                cur.execute(creation_query)<br>
                for row in pd.read_excel(os.path.join(data_folder, kt_cleaned), sheetname='creative_cleaning').iterrows():<br>
                    cur.execute(insert_query % (row[1]['kt_creative_id'], row[1]['kt_creative_clean']))<br>
                connection.commit()<br>
        </code></p>
        """.format(file, table, process)
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


def main():
    # Analysis covers last six weeks data
    today = datetime.date.today()
    start_date = (today - datetime.timedelta(weeks=6, days=1)).strftime('%Y-%m-%d')
    end_date = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    output_table = 'incampaign_kt_creative_cleaned'
    flag = 'kt_creative_cleaned'

    # Location of sources and destination files
    output_folder = ROOT_FOLDER + 'RenTrak'
    output_file = 'kt_cleaned_%s_%s.xlsx' % (start_date, end_date)
    # sheet_name = 'creative_cleaning'
    s3_folder_name = 'RenTrak/CreativeCleaned/'

    if not os.path.exists(output_folder):
        print("Creating a new local folder for export file:", output_folder)
        os.makedirs(output_folder)

    # Step 1: Download all possible KT combinations and current matching cleaned creative names
    extract_query = """
        SELECT Air_ISCI as kt_creative_id, Cmml_Title AS kt_creative, kt_creative_clean
        FROM gaintheory_us_targetusa_14.Keepingtrac_backup a
        LEFT JOIN gaintheory_us_targetusa_14.{0} b
        ON a.Air_ISCI = b.kt_creative_id
        WHERE Air_ISCI IS NOT NULL
        GROUP BY a.Air_ISCI, a.Cmml_Title, kt_creative_clean
        ORDER BY kt_creative_id
    """.format(output_table)

    df = vertica_extract(
        extract_query,
        ['kt_creative_id', 'kt_creative', 'kt_creative_clean']
    )
    df = df.dropna(how='any') # drop blank rows
    unmapped_creatives = sum(x == 'nan' for x in df['kt_creative_clean'])

    if unmapped_creatives == 0: ## TODO: change back to > 0
        print("Generating unmapped kt_creatives for date ranging between", start_date, "and", end_date)
        df.to_excel(
            os.path.join(output_folder, output_file),
            #sheet_name=sheet_name,
            index=False
        )

        # Export to S3
        file_to_export = output_folder + "/"+ output_file
        if os.path.isfile(file_to_export):
            s3 = client('s3')
            s3_outfile_name = s3_folder_name + output_file
            s3.upload_file(file_to_export, EXPORT_BUCKET, s3_outfile_name)
            print("File exported to S3 location=>", s3_outfile_name)

            #os.remove(file_to_export)
            print("Deleted local file=>", file_to_export)

        # Send email to tell the team to start manual mapping
        subject = "RenTrak automated processing: new kt_creatives need to be mapped"
        body = notify_for_manual_mapping(output_file, output_table, 'kt_creative_cleaned')
        send_notification_email(ADMIN_EMAIL_RECIPIENTS, subject, body, file_to_export) # TODO: update recipients
        print("Notified the team to add manual mapping")
    else:
        print("Everything is mapped")

        # create_table = """
        #     DROP TABLE IF EXISTS gaintheory_us_targetusa_14.{0};
        #     CREATE TABLE gaintheory_us_targetusa_14.{0} (
        #             kt_creative_id   varchar(150),
        #             kt_creative_clean varchar(1500)
        #     );
        # """.format(output_table)
        #
        # insert_mappings = """
        #     INSERT INTO gaintheory_us_targetusa_14.{0}(kt_creative_id, kt_creative_clean) VALUES ('%s', '%s')
        # """.format(output_table)
        # # import pdb
        # # pdb.set_trace()
        # with vertica_python.connect(**conn_info) as connection:
        #     cur = connection.cursor()
        #     cur.execute(create_table)
        #     for row in pd.read_excel(os.path.join(output_folder, output_file)).iterrows():#sheetname=sheet_name).iterrows():
        #         cur.execute(insert_mappings % (row[1]['kt_creative_id'], row[1]['kt_creative_clean']))
        #     connection.commit()
        # print("New mapping data inserted to the table:", output_table)

        # TODO: reset flag to 1
        print("Set flag:", flag, "to 1 so that the second part of RenTrak processing can proceed")

        # # insert, set flag to 1 and send email notification about being cleaned
        # subject = "RenTrak automated processing: new kt_creatives need to be mapped"
        # body = notify_for_manual_mapping(output_file, output_table, 'kt_creative_cleaned')
        # send_notification_email(ADMIN_EMAIL_RECIPIENTS, subject, body) # TODO: update recipients
        print("Notified the team that mappings are added")

if __name__ == "__main__":
    main()
