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
            a.fb_date >= (GETDATE()-60)::DATE
    );
    """.format(output_table, schema_name)

def collect_any_unmatched_campaign_gp_names(output_table, input_table, schema_name):
    return """
    CREATE TABLE {2}.{0} AS
    (
        SELECT fb_campaign_group_name
        FROM {2}.{1}
        WHERE fb_mapped_campaign_group_name IS NULL
    );
    """.format(output_table, input_table, schema_name)

def collect_matched_aggregated_data(output_table, input_table1, schema_name):
    return """
    CREATE TABLE {2}.{0} AS
    (
        SELECT
            a.fb_mapped_campaign_group_name,
            a.fb_dma_code,
            a.fb_dma_name,
            a.fb_date,
            SUM(a.fb_total_impressions)           AS fb_total_impressions,
            SUM(a.fb_mobile_impressions)          AS fb_mobile_impressions,
            SUM(a.fb_desktop_impressions)         AS fb_desktop_impressions,
            SUM(a.fb_newsfeed_impressions)        AS fb_newsfeed_impressions,
            SUM(a.fb_rhs_impressions)             AS fb_rhs_impressions,
            SUM(a.fb_instagram_impressions)       AS fb_instagram_impressions,
            SUM(a.fb_audiencenetwork_impressions) AS fb_audiencenetwork_impressions
        FROM
            {2}.{1} AS a
        WHERE
            fb_mapped_campaign_group_name IS NOT NULL
        GROUP BY
            fb_mapped_campaign_group_name,
            fb_dma_code,
            fb_dma_name,
            fb_date
    );
    """.format(output_table, input_table1, schema_name)

def allocate_dma_and_population_wt(output_table, input_table1, schema_name):
    return """
    CREATE TABLE {2}.{0} AS
    (
        SELECT
            a.fb_mapped_campaign_group_name,
            a.fb_dma_code,
            b.incampaign_zipcode AS fb_zipcode,
            a.fb_dma_name,
            a.fb_date,
            b.incampaign_wt_population,
            (b.incampaign_wt_population * a.fb_mobile_impressions)          AS fb_mobile_impressions,
            (b.incampaign_wt_population * a.fb_desktop_impressions)         AS fb_desktop_impressions,
            (b.incampaign_wt_population * a.fb_newsfeed_impressions)        AS fb_newsfeed_impressions,
            (b.incampaign_wt_population * a.fb_rhs_impressions)             AS fb_rhs_impressions,
            (b.incampaign_wt_population * a.fb_instagram_impressions)       AS fb_instagram_impressions,
            (b.incampaign_wt_population * a.fb_audiencenetwork_impressions) AS
                                                                     fb_audiencenetwork_impressions,
            (b.incampaign_wt_population * a.fb_total_impressions) AS fb_total_impressions
        FROM
            {2}.{1} AS a
        LEFT JOIN
            {2}.incampaign_dma_to_zipcode_and_population_weight AS b
        ON
            a.fb_dma_code = b.incampaign_dmac
        WHERE
            b.incampaign_dmac IS NOT NULL
    );
    """.format(output_table, input_table1, schema_name)

def create_transform_table(output_table, input_table1, schema_name):
    return """
    CREATE TABLE {2}.{0} AS
    (
        SELECT
            'Geo_'||fb_zipcode            AS Geography,
            'Target'                      AS Product,
            fb_mapped_campaign_group_name AS Campaign,
            'Total_FB_Imp'                AS VariableName,
            'Total'                       AS Outlet,
            'Total'                       AS Creative,
            fb_date::DATE                 AS Period,
            SUM(fb_total_impressions)     AS VariableValue
        FROM
            {2}.{1}
        GROUP BY
            Geography,
            Product,
            Campaign,
            VariableName,
            Outlet,
            Creative,
            Period
    );
    """.format(output_table, input_table1, schema_name)


def send_completion_email(table_name):
    subject = "InCampaign Facebook Script#2 finished"
    body = """
    <p>InCampaign Facebook Script#2 has just finished.</p>
    <p>The final transformed table is written as <b>{0}</b> in Vertica backend.</p>
    <p>Next, we will attempt to move that data into S3.</p>
    """.format(table_name)
    Mailer().send_email(ONSHORE_EMAIL_RECIPIENTS, subject, body)
    print("Successful completion email sent.")


def send_error_email(log_table, error_msg):
    subject = "ERROR in InCampaign Facebook Script#2"
    body = """
    <p>InCampaign Facebook Script#2 has run into error as shown below:</p>
    <p><strong style="color: red;">{1}</strong></p>
    <p>For SQL script error, you can also look at which step the error occurred by checking the <b>{0}</b> table</p>
    """.format(log_table, error_msg)
    Mailer().send_email(ERROR_EMAIL_RECIPIENTS, subject, body)
    print("Error notification email sent")

def main():
    recipients = ['phyo.thiha@groupm.com']  # , 'jessie.zhang@groupm.com']

    schema_name = 'gaintheory_us_targetusa_14'
    log_table = 'incampaign_facebook_log'
    all_cmp_gp_to_map = 'incampaign_temp_facebook_all_campaign_group_names_to_map'
    final_transform_table = 'incampaign_facebook_last_60_days'

    imp_joined_camp_gp_mappings = 'incampaign_temp_facebook_impressions_lj_campaign_gp_mappings'
    new_cmp_gp_to_map = 'incampaign_temp_facebook_new_campaign_gp_names_to_map'
    aggregated_mapped_table = 'incampaign_temp_facebook_aggregated_impressions_mapped_by_campaign_gp_names'
    dma_allocated_table = 'incampaign_temp_facebook_dma_to_zip_allocated_and_population_weight_applied'

    logger = Logger(log_table, schema_name)

    try:
        with vertica_python.connect(**conn_info) as connection:
            print("Started script#2 of facebook data processing...")
            cursor = connection.cursor()

            row_cnt = get_row_count(cursor, all_cmp_gp_to_map, schema_name)
            if row_cnt[0] != 0:
                error_msg = "Row count of " + all_cmp_gp_to_map + " is not zero. Someone probably forgot to update the mappings and truncate this table."
                logger.log(cursor,'Facebook STEP 4: ', error_msg)

            drop_table(cursor, imp_joined_camp_gp_mappings, schema_name)
            drop_table(cursor, new_cmp_gp_to_map, schema_name)
            drop_table(cursor, aggregated_mapped_table, schema_name)
            drop_table(cursor, dma_allocated_table, schema_name)
            logger.log(cursor, 'Facebook STEP 4: ',
                       'Dropped temp tables that will be used before Script 2 processing')

            logger.log(cursor,'Facebook STEP 5 START', 'Join impressions with (hopefully) updated campaign_gp mappings')
            cursor.execute(join_impressions_with_campaign_gp_names(imp_joined_camp_gp_mappings, schema_name))
            row_cnt = str(get_row_count(cursor, imp_joined_camp_gp_mappings, schema_name))
            logger.log(cursor, 'Facebook STEP 5 END', imp_joined_camp_gp_mappings + ' => row count: ' + row_cnt)

            logger.log(cursor, 'Facebook STEP 6 START', 'Extract campaign group names that cannot be mapped')
            cursor.execute(collect_any_unmatched_campaign_gp_names(new_cmp_gp_to_map, imp_joined_camp_gp_mappings, schema_name))

            row_cnt = get_row_count(cursor, new_cmp_gp_to_map, schema_name)
            if row_cnt[0] != 0:
                error_msg = "Facebook STEP 6 ERROR: Row count of " + new_cmp_gp_to_map + \
                            " is not zero. This means we still have some campaign mappings to be mapped and loaded from that table to 'InCampaign_Facebook_Campaign_Group_Name_Mapping' feed in DataVault"
                print(error_msg)
                send_error_email(recipients, log_table, error_msg)
                logger.log(cursor,'Facebook STEP 6 END: ', error_msg)
                exit(1)

            logger.log(cursor, 'Facebook STEP 7 START', 'Extract and aggregate facebook data that are fully mapped')
            cursor.execute(collect_matched_aggregated_data(aggregated_mapped_table, imp_joined_camp_gp_mappings, schema_name))
            row_cnt = str(get_row_count(cursor, aggregated_mapped_table, schema_name))
            logger.log(cursor, 'Facebook STEP 7 END', aggregated_mapped_table + ' => row count: ' + row_cnt)

            logger.log(cursor, 'Facebook STEP 8 START', 'Allocate DMA to Zipcode mapping and population weight')
            cursor.execute(allocate_dma_and_population_wt(dma_allocated_table, aggregated_mapped_table, schema_name))
            row_cnt = str(get_row_count(cursor, dma_allocated_table, schema_name))
            logger.log(cursor, 'Facebook STEP 8 END', dma_allocated_table + ' => row count: ' + row_cnt)

            logger.log(cursor, 'Facebook STEP 9 START', 'Create Transform Table')
            cursor.execute(create_transform_table(final_transform_table, dma_allocated_table, schema_name))
            row_cnt = str(get_row_count(cursor, final_transform_table, schema_name))
            logger.log(cursor, 'Facebook STEP 9 END', final_transform_table + ' => row count: ' + row_cnt)

            drop_table(cursor, imp_joined_camp_gp_mappings, schema_name)
            drop_table(cursor, new_cmp_gp_to_map, schema_name)
            drop_table(cursor, aggregated_mapped_table, schema_name)
            drop_table(cursor, dma_allocated_table, schema_name)
            logger.log(cursor, 'Facebook STEP 10', 'Dropped all temporary tables used in Script 2 processing')

            send_completion_email(final_transform_table)
            print("Finished script#2 of facebook data processing.")

    except vertica_python.errors.QueryError as err:
        print("Vertica Query Error!")
        send_error_email(log_table, err)
    except Exception as err:
        print("Unknown Error Occurred!: ", err)
        send_error_email(log_table, err)

if __name__ == "__main__":
    main()