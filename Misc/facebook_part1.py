"""
This script exports new (unmapped) campaign_group_names from Facebook data
and alert the user (e.g., Manoj) via email to do the manual mapping before proceeding
with further steps.

Author: Phyo Thiha
Last Modified: Nov 16, 2016
"""
from account_info import *
from vertica_utils import *
from logger import Logger
from mailer import Mailer

def join_impressions_with_campaign_gp_names(output_table, schema_name):
    return """
    CREATE TABLE {1}.{0} AS
    (
        SELECT
            a.fb_campaign_group_name,
            b.fb_mapped_campaign_group_name,
            a.fb_dma_code,
            a.fb_dma_name,
            a.fb_date,
            a.fb_total_impressions,
            a.fb_mobile_impressions,
            a.fb_desktop_impressions,
            a.fb_newsfeed_impressions,
            a.fb_rhs_impressions,
            a.fb_instagram_impressions,
            a.fb_audiencenetwork_impressions
        FROM
            {1}.incampaign_facebook_impressions_zipcode AS a
        LEFT JOIN
            {1}.incampaign_facebook_mapped_campaign_group_name AS b
        ON
            a.fb_campaign_group_name = b.fb_campaign_group_name
        WHERE
            a.fb_date >= (GETDATE()-60)::DATE );
    """.format(output_table, schema_name)

def collect_new_campaign_gp_names_to_map(output_table, input_table1, schema_name):
    return """
    CREATE TABLE {2}.{0} AS
    (
        SELECT DISTINCT
            fb_campaign_group_name
        FROM
            {2}.{1} AS a
        WHERE
            fb_mapped_campaign_group_name IS NULL );
    """.format(output_table, input_table1, schema_name)

def upsert_unseen_campaign_gp_names(output_table, input_table1, schema_name):
    return """
    CREATE TABLE
    IF NOT EXISTS {2}.{0}
    (
        fb_campaign_group_name VARCHAR(1000)
    );

    MERGE
    INTO
        {2}.{0} AS t
    USING
        {2}.{1} AS s
    ON
        t.fb_campaign_group_name = s.fb_campaign_group_name
    WHEN MATCHED
        THEN
    UPDATE
    SET
        fb_campaign_group_name = s.fb_campaign_group_name
    WHEN NOT MATCHED
        THEN
    INSERT
        (
            fb_campaign_group_name
        )
        VALUES
        (
            s.fb_campaign_group_name
        );
    """.format(output_table, input_table1, schema_name)

def send_completion_email(table_name):
    subject = "InCampaign Facebook Script#1 finished: Waiting for campaign group name mappings to be loaded via DataVault"
    body = """
    <p>InCampaign Facebook Script#1 has just finished.</p>
    <p>Before running the Python script#2 for Facebook (facebook_part2.py), please <ol>
    <li>download <b>{0}</b> table from Vertica backend</li>
    <li>fill in the mappings in a CSV file</li>
    <li>finally, upload that updated mapping file via 'InCampaign_Facebook_Campaign_Group_Name_Mapping' feed in DataVault.</li></p>
    <p>After that, <strong style="color: red;">MAKE SURE TO empty (truncate) the {0} table</strong>.</p>
    """.format(table_name)
    Mailer().send_email(ONSHORE_EMAIL_RECIPIENTS, subject, body)
    print("Successful completion email sent.")

def send_error_email(log_table, error_msg):
    subject = "ERROR in InCampaign Facebook Script#1"
    body = """
    <p>InCampaign Facebook Script#1 has run into error as shown below:</p>
    <p><strong style="color: red;">{1}</strong></p>
    <p>For SQL script error, you can also look at which step the error occurred by checking the <b>{0}</b> table</p>
    """.format(log_table, error_msg)
    Mailer().send_email(ERROR_EMAIL_RECIPIENTS, subject, body)
    print("Error notification email sent")

def main():
    schema_name = 'gaintheory_us_targetusa_14'
    log_table = 'incampaign_facebook_log'
    imp_joined_camp_gp_mappings = 'incampaign_temp_facebook_impressions_lj_campaign_gp_mappings'
    new_cmp_gp_to_map = 'incampaign_temp_facebook_new_campaign_gp_names_to_map'
    all_cmp_gp_to_map = 'incampaign_temp_facebook_all_campaign_group_names_to_map'

    logger = Logger(log_table, schema_name)

    try:
        with vertica_python.connect(**conn_info) as connection:
            print("Started script#1 of facebook data processing...")
            cursor = connection.cursor()

            drop_table(cursor, imp_joined_camp_gp_mappings, schema_name)
            drop_table(cursor, new_cmp_gp_to_map, schema_name)

            logger.log(cursor,'Facebook STEP 1 START', 'Join impressions with existing campaign_gp mappings')
            cursor.execute(join_impressions_with_campaign_gp_names(imp_joined_camp_gp_mappings, schema_name))
            row_cnt = str(get_row_count(cursor, imp_joined_camp_gp_mappings, schema_name))
            logger.log(cursor, 'Facebook STEP 1 END', imp_joined_camp_gp_mappings + ' => row count: ' + row_cnt)

            logger.log(cursor, 'Facebook STEP 2 START', 'Extract new campaign group names to map')
            cursor.execute(collect_new_campaign_gp_names_to_map(new_cmp_gp_to_map, imp_joined_camp_gp_mappings, schema_name))
            row_cnt = str(get_row_count(cursor, new_cmp_gp_to_map, schema_name))
            logger.log(cursor,'Facebook STEP 2 END', new_cmp_gp_to_map + ' => row count: ' + row_cnt)

            logger.log(cursor, 'Facebook STEP 3 START', 'Upsert new campaign group names to to_map table')
            cursor.execute(upsert_unseen_campaign_gp_names(all_cmp_gp_to_map, new_cmp_gp_to_map, schema_name))
            row_cnt = str(get_row_count(cursor, all_cmp_gp_to_map, schema_name))
            logger.log(cursor, 'Facebook STEP 3 END', all_cmp_gp_to_map + ' => row count: ' + row_cnt)

            send_completion_email(all_cmp_gp_to_map)
            print("Finished script#1 of facebook data processing.")

    except vertica_python.errors.QueryError as err:
        print("Vertica Query Error!")
        send_error_email(log_table, str(err))
    except Exception as err:
        print("Unknown Error Occurred!")
        send_error_email(log_table, str(err))

if __name__ == "__main__":
    main()