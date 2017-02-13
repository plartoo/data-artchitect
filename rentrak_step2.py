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
        <b>(If you have done this already for most recent KeepingTrac data, skip this step) </b>
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


def update_reference_query():
    return """
      DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_kt_with_rt_network;
        CREATE TABLE gaintheory_us_targetusa_14.incampaign_tmp_kt_with_rt_network AS
        SELECT  CASE
                        WHEN Air_Time < '05:00:00' THEN Air_Date + 1
                        ELSE Air_date
                END AS Air_Date,
                Air_Time,
                network AS kt_network,
                rt_network,
                Air_ISCI AS kt_creative_id,
                kt_creative_clean AS kt_creative,
                Spot_Length,
                Act_Impression
        FROM
            gaintheory_us_targetusa_14.incampaign_keepingtrac_all kt
        LEFT JOIN gaintheory_us_targetusa_14.js_rt_kt_reference rf
        ON kt.network = rf.kt_network
        LEFT JOIN gaintheory_us_targetusa_14.incampaign_kt_creative_mappings cr
        ON kt.Air_ISCI = cr.kt_creative_id
        WHERE Air_Date IS NOT NULL
        AND Type_of_Demographic = 2
        AND NOT Media_Type = 'Syndication';

        SELECT *
        FROM gaintheory_us_targetusa_14.incampaign_tmp_kt_with_rt_network;
    """


def get_unmapped_creative_query(start_date, end_date):
    return """
    DROP TABLE IF EXISTS gaintheory_us_targetusa_14.incampaign_tmp_kt_creative_dedupe_raw;
    CREATE TABLE gaintheory_us_targetusa_14.incampaign_tmp_kt_creative_dedupe_raw AS
    (
        SELECT rt_creative_id, rt_creative, kt_creative_id, kt_creative, SUM(rt_imp) AS rt_imp, SUM(kt_imp) AS kt_imp
        FROM (
                SELECT DATE_TRUNC('minute', Air_Date + Air_Time) AS air_date, rt_network AS network, kt_creative_id, kt_creative, SUM(Act_Impression) as kt_imp
                FROM gaintheory_us_targetusa_14.incampaign_tmp_kt_with_rt_network
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
        ORDER BY rt_creative_id, kt_creative_id
    );


    SELECT *
    FROM gaintheory_us_targetusa_14.incampaign_tmp_kt_creative_dedupe_raw;
    -- Select the ones from raw data which we don't have mappings already or are currently set as 'unknown'
    -- NOTE: removed for now because it's easier to QA without this against Manoj's manual mappings
    -- SELECT a.rt_creative_id, a.rt_creative, a.kt_creative_id, a.kt_creative, a.rt_imp, a.kt_imp
    -- FROM
    --     gaintheory_us_targetusa_14.incampaign_tmp_kt_creative_dedupe_raw a
    -- LEFT JOIN
    --     gaintheory_us_targetusa_14.incampaign_tmp_creative_match_deduped b
    -- ON
    --     a.rt_creative_id = b.rt_creative_id
    -- WHERE
    --     b.kt_creative IS NULL
    -- OR  b.kt_creative = 'unknown'
    -- GROUP BY a.rt_creative_id, a.rt_creative, a.kt_creative_id, a.kt_creative, a.rt_imp, a.kt_imp
    -- ORDER BY a.rt_creative_id, a.rt_imp, a.kt_imp DESC
    """.format(start_date, end_date)


def get_mapped_creatives():
    return"""
    SELECT a.rt_creative_id, a.rt_creative, a.kt_creative_id, a.kt_creative
    FROM
        gaintheory_us_targetusa_14.incampaign_rentrak_creative_match_deduped a
    GROUP BY a.rt_creative_id, a.rt_creative, a.kt_creative_id, a.kt_creative
    """


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


def dedupe(raw_cr, ref_cr):
    """
    Dedupe results in dataframe using based on what Manoj's team wanted.
    :param dataframe: Pandas dataframe as input
    :return: dataframe: deduped Pandas dataframe
    """
    columns = raw_cr.columns
    deduped_df = pd.DataFrame(columns=columns)

    for name, gp in raw_cr.groupby('rt_creative_id'):
        sorted_gp = gp.sort_values(['rt_imp', 'kt_imp'], ascending=False)

        j = 0
        final_kt_creative = 'unknown'
        cur_rt_creative_id = int(sorted_gp.iloc[j].rt_creative_id) # pandas somehow turns ints into strs
        first_row = sorted_gp.iloc[j].values.tolist()

        while (j < len(sorted_gp)):
            cur_row = sorted_gp.iloc[j]
            if not pd.isnull(cur_row.kt_creative):
                final_kt_creative = cur_row.kt_creative
                break
            j += 1

        #http://stackoverflow.com/questions/30787901/how-to-get-a-value-from-a-pandas-dataframe-and-not-the-index-and-object-type
        if final_kt_creative == 'unknown': # if still 'unknown', then look up in historical reference
            historical_record = ref_cr.loc[ref_cr.rt_creative_id==cur_rt_creative_id].values.tolist()
            # e.g., [[1945979, '30% Off Toys', 'QDAF0014778', 'Holiday 2016 Ginger Toys Big Selfie']]
            if historical_record:
                final_kt_creative = historical_record[0][-1]

        first_row[3] = final_kt_creative
        d = dict(zip(columns, first_row))
        deduped_df = deduped_df.append(d, ignore_index=True)

    return deduped_df


def main():
    today = datetime.datetime.now()
    start_date = (today - datetime.timedelta(days=61)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')

    print(start_date, end_date)

    schema_name = 'gaintheory_us_targetusa_14'
    tmp_table = 'incampaign_tmp_creative_match_deduped'
    deduped_table = 'incampaign_rentrak_creative_match_deduped'

    flag_table = 'incampaign_process_switches'
    flag_to_set = 'rentrak_creative_match_deduped' # if flag is not checked, alert team and let them know to process kt_data
    flag_to_check = 'rentrak_kt_creative_cleaned'

    # Acquire the lock
    set_lock(flag_table, schema_name, flag_to_set, 0)

    # Create/Update the reference table (copy creative names to KT from RenTrak): incampaign_tmp_kt_with_rt_network
    df = vertica_extract(update_reference_query(), ['Air_Date', 'Air_Time',
                                                    'kt_network', 'rt_network',
                                                    'kt_creative_id', 'kt_creative',
                                                    'Spot_Length', 'Act_Impression'
                                                    ])

    # Download the raw creative details from both KT and RT and match based on network and minute
    raw_creatives = vertica_extract(get_unmapped_creative_query(start_date, end_date),
                                ['rt_creative_id', 'rt_creative', 'kt_creative_id', 'kt_creative', 'rt_imp', 'kt_imp'])
    # raw_creatives.to_excel(os.path.join('RenTrak', 'dupes.xlsx'),index=False)
    reference_creatives = vertica_extract(get_mapped_creatives(), ['rt_creative_id', 'rt_creative',
                                                                   'kt_creative_id', 'kt_creative'])

    deduped_creatives = dedupe(raw_creatives, reference_creatives)
    insert_from_dataframe(deduped_creatives, tmp_table, schema_name)
    merge_from_tmp_to_final_deduped_table(tmp_table, deduped_table, schema_name)

    flag_val = get_lock_value(flag_table, schema_name, flag_to_check)
    if flag_val == 0:
        print(flag_to_check, "is set to:", flag_val)
        subject = "RenTrak automated processing step 2 (needs attention): kt_creatives might not have been uploaded"
        body = notify_to_set_flags({'flag': flag_to_set})
        send_notification_email(ONSHORE_EMAIL_RECIPIENTS, subject, body)
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
        send_notification_email(ONSHORE_EMAIL_RECIPIENTS, subject, body)
        print("Notified the team that no further action on their part is required")


if __name__ == "__main__":
    main()
