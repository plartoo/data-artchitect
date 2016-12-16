"""
TODO: update descriptino after everything is concrete
Description: Script to extract KeepingTrac's creative names and send
team notification to start manual mapping as necessary.

This step must happen BEFORE the processing of deduping of RenTrak
creative names (step 2 in RenTrak processing).
"""
import datetime
import pandas as pd

from mailer import Mailer
from vertica_utils import *
from s3_utils import *


def notify_to_set_flags(flag_name):
    email_str = """
        <p>Python script successfully deduped creative matches from RenTrak data.</p>
        <p>To proceed to the final step of extracting the last 60 days' data and exporting it to S3, we must:
        <ol>
        <li>
        <b>(If you have done it already, skip this step) </b>
        Using DataVault's <b>'InCampaign KT Creative Mappings'</b> feed, upload the kt_creative mappings,
        which should have been sent to you via this automated process a few days ago.
        If you have not received it for this week, please ask Phyo to generate it for you..</li>
        <li>
            <span style="color: red;">AFTER you've ensured that latest kt creative mappings are uploaded in DataVault,
            </span>
        run the SQL query below in Vertica backend:
        <br>
        <br>
            <b>
            UPDATE gaintheory_us_targetusa_14.incampaign_process_switches
            SET run = 1
            WHERE process_name = '{flag}';
            </b>
        </li>
        </ol>
        </p>
        """.format(**flag_name)
    return email_str


def notify_success(table_name):
    email_str = """
        <p>Python script successfully deduped and uploaded new creative_match data for RenTrak to this table: <b></b>.
        The final step of extracting the last 60 days' data and exporting it to S3 will begin soon and you'll receive
        another email once it finishes.
        </p>
        <p><b>No further action on your part is needed.</b></p>
        """.format(table_name)
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


def get_flag_value(table_name, schema_name, flag_name):
    return """
    SELECT
        run
    FROM
        {1}.{0}
    WHERE
        process_name = '{2}'
    """.format(table_name, schema_name, flag_name)


def get_lock_value(table_name, schema_name, flag_name):
    with vertica_python.connect(**conn_info) as connection:
        cur = connection.cursor()
        cur.execute(get_flag_value(table_name, schema_name, flag_name))
        val = cur.fetchall()
        val = -1 if not val else val[0][0]
    return val


def get_creative_query(start_date, end_date):
    return """
    SELECT rt_creative_id, rt_creative, kt_creative_id, kt_creative, SUM(rt_imp) AS rt_imp, SUM(kt_imp) AS kt_imp
    FROM (
            SELECT DATE_TRUNC('minute', Air_Date + Air_Time) AS air_date, rt_network AS network, kt_creative_id, kt_creative, SUM(Act_Impression) as kt_imp
            FROM gaintheory_us_targetusa_14.incampaign_tmp_kt_with_rt_network_mk
            WHERE Air_Date BETWEEN '{0}' AND '{1}'
            GROUP BY DATE_TRUNC('minute', Air_Date + Air_Time), network, kt_creative_id, kt_creative
    ) kt
    FULL OUTER JOIN(
            SELECT DATE_TRUNC('minute', rentrak_ad_time) AS air_date,
                    rentrak_network as network, rentrak_ad_no AS rt_creative_id,
                    rentrak_ad_copy AS rt_creative, SUM(a.rentrak_ad_zip_aa) AS rt_imp
            FROM gaintheory_us_targetusa_14.incampaign_rentrak_zipcode a
            LEFT JOIN gaintheory_us_targetusa_14.incampaign_rentrak_spotid b
            ON a.rentrak_spot_id = b.rentrak_spot_id and a.rentrak_week = b.rentrak_week
            WHERE rentrak_ad_time::date BETWEEN '{0}' AND '{1}'
            GROUP BY DATE_TRUNC('minute', rentrak_ad_time), network, rt_creative_id, rt_creative
    ) rt
    ON kt.air_date = rt.air_date AND kt.network = rt.network
    WHERE  rt_creative_id IS NOT NULL
    GROUP BY rt_creative_id, rt_creative, kt_creative_id, kt_creative
    ORDER BY rt_creative_id, kt_creative_id;
    """.format(start_date, end_date)


def insert_query(table_name, schema_name):
    return """
        INSERT INTO {1}.{0} (rt_creative_id, rt_creative, kt_creative_id, kt_creative) VALUES (%s, %s, %s, %s)
    """.format(table_name, schema_name)


def table_create_query(table_name, schema_name):
    return """
        DROP TABLE IF EXISTS {1}.{0};
        CREATE TABLE {1}.{0}(
                rt_creative_id   integer,
                rt_creative      varchar(1500),
                kt_creative_id   varchar(150),
                kt_creative varchar(1500)
        );
    """.format(table_name, schema_name)


def insert_from_dataframe(dataframe, table_name, schema_name):
    cq = table_create_query(table_name, schema_name)
    iq = insert_query(table_name, schema_name)

    with vertica_python.connect(**conn_info) as connection:
        cur = connection.cursor()
        cur.execute(cq)
        for index, row in dataframe.iterrows():
            cur.execute(iq, (row.rt_creative_id, row.rt_creative, row.kt_creative_id, row.kt_creative))
        connection.commit()


def merge_query(dest_table, target_table, schema_name):
    return """
        MERGE INTO
            {2}.{1} AS a
        USING
            {2}.{0} AS b
        ON
            a.rt_creative_id = b.rt_creative_id
        WHEN MATCHED
            THEN UPDATE
                SET
                    rt_creative = b.rt_creative,
                    kt_creative_id = b.kt_creative_id,
                    kt_creative = b.kt_creative
        WHEN NOT MATCHED
            THEN INSERT
                (rt_creative_id, rt_creative, kt_creative_id, kt_creative)
            VALUES
                (b.rt_creative_id, b.rt_creative, b.kt_creative_id, b.kt_creative);
    """.format(dest_table, target_table, schema_name)


def merge_from_tmp_to_final_deduped_table(tmp_table, mapping_table, schema_name):
    with vertica_python.connect(**conn_info) as connection:
        cur = connection.cursor()
        cur.execute(merge_query(tmp_table, mapping_table, schema_name))
        connection.commit()


def dedupe(dataframe):
    """
    Dedupe results in dataframe using based on what Manoj's team wanted.
    :param dataframe: Pandas dataframe as input
    :return: dataframe: deduped Pandas dataframe
    """
    columns = dataframe.columns
    deduped_df = pd.DataFrame(columns=columns)

    for name, gp in dataframe.groupby('rt_creative_id'):
        sorted_gp = gp.sort_values(['rt_imp', 'kt_imp'], ascending=False)

        j = 0
        final_kt_creative = 'unknown'
        cur_row = sorted_gp.iloc[j]
        first_row = cur_row.values.tolist()

        if not pd.isnull(cur_row.kt_creative):
            final_kt_creative = cur_row.kt_creative
        else:
            while (j < len(sorted_gp)) and (pd.isnull(cur_row.kt_creative)):
                cur_row = sorted_gp.iloc[j]
                if not pd.isnull(cur_row.kt_creative):
                    final_kt_creative = cur_row.kt_creative
                    break
                j += 1

        first_row[3] = final_kt_creative

        d = dict(zip(columns, first_row))
        deduped_df = deduped_df.append(d, ignore_index=True)

    return deduped_df


def main():
    today = datetime.datetime.now()
    start_date = (today - datetime.timedelta(days=61)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')

    schema_name = 'gaintheory_us_targetusa_14'
    tmp_table = 'incampaign_tmp_js_creative_match_deduped'
    deduped_table = 'incampaign_rentrak_creative_match_deduped'

    flag_table = 'incampaign_process_switches'
    flag_to_set = 'rentrak_creative_match_deduped' # if flag is not checked, alert team and let them know to process kt_data
    flag_to_check = 'rentrak_kt_creative_cleaned'

    # Acquire the lock
    set_lock(flag_table, schema_name, flag_to_set, 0)

    # Download the raw creative details from both KT and RT and match based on network and minute
    dataframe = vertica_extract(get_creative_query(start_date, end_date),
                                ['rt_creative_id', 'rt_creative', 'kt_creative_id', 'kt_creative', 'rt_imp', 'kt_imp'])
    dataframe = dedupe(dataframe)
    insert_from_dataframe(dataframe, tmp_table, schema_name)
    merge_from_tmp_to_final_deduped_table(tmp_table, deduped_table, schema_name)

    flag_val = get_lock_value(flag_table, schema_name, flag_to_check)
    if flag_val == 0:
        print(flag_to_check, "is set to:", flag_val)
        subject = "RenTrak automated processing step 2 (needs attention): kt_creatives might not have been uploaded"
        body = notify_to_set_flags({'flag': flag_to_set})
        send_notification_email(DEV_EMAIL_RECIPIENTS, subject, body) # TODO: replace with ONSHORE_EMAIL_RECIPIENTS
        print("Notified the team about kt_creative flag")
    elif flag_val < 0:
        print(flag_to_check, "is set to:", flag_val)
        subject = "RenTrak automated processing step 2 (needs dev attention): " + flag_to_check + "is set to -1"
        body = """
            Troubleshoot why the value of this flag is returned as -1 by rentrak_step2 code.
            After that, set '{0}' to 0 and '{1}' to 1 so that step 3 can automatically proceed
            """.format(flag_to_check, flag_to_set)
        send_notification_email(DEV_EMAIL_RECIPIENTS, subject, body)
        print("Notified the team to about kt_creative flag")
    else:
        set_lock(flag_table, schema_name, flag_to_check, 0) # we've got here because it was 1, now set it back to 0
        set_lock(flag_table, schema_name, flag_to_set, 1) # this flag is set to 1 so that step 3 can proceed

        print(flag_to_check, "is set to:", flag_val)
        subject = "RenTrak automated processing step 2 successfully completed (no follow-up action required)"
        body = notify_success(deduped_table)
        send_notification_email(DEV_EMAIL_RECIPIENTS, subject, body) # TODO: replace with ONSHORE_EMAIL_RECIPIENTS
        print("Notified the team that no further action on their part is required")

if __name__ == "__main__":
    main()
