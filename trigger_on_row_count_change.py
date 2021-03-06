from account_info import *
from vertica_utils import *
from mailer import Mailer

import subprocess


def send_dev_email(table_name, row_count, cmd_used, status_msg):
    status_msg = str(status_msg).replace('\\r\\n', '<br>')
    subject = "Row count has changed for: " + table_name
    body = """
    <p>Because row count for table {0} has changed to {1},</p>
    <p>we ran: {2} </p><br>
    <p>That finished with the following message: {3}</p>
    """.format(table_name, row_count, cmd_used, status_msg)
    Mailer().send_email(DEV_EMAIL_RECIPIENTS, subject, body)
    print("Admin notification email sent.")


def send_notification_email(recipients, subject, body, attachment=None):
    Mailer().send_email(recipients, subject, body, attachment)
    print("Notification email sent.")


def send_error_email(error_msg):
    error_msg = str(error_msg).replace('\\r\\n', '<br>')
    subject = "ERROR in trigger_on_row_count_change.py"
    body = """
    <p>trigger_on_row_count_change.py script has run into error below:</p>
    <p><strong style="color: red;">{0}</strong></p>
    """.format(error_msg)
    Mailer().send_email(ERROR_EMAIL_RECIPIENTS, subject, body)
    print("Error notification email sent")


def get_most_recent_row_cnt(row_cnt_table, schema_name, table_name):
    return """
        SELECT *
        FROM {1}.{0}
        WHERE table_name='{2}'
        ORDER BY event_time DESC
        LIMIT 1
    """.format(row_cnt_table, schema_name, table_name)


def increase_observation_cnt(row_cnt_table, schema_name, table, cur_row_cnt, datetime_to_micro_sec, new_obsv_cnt):
    return """
        UPDATE {1}.{0}
        SET num_of_times_observed={5}
        WHERE table_name='{2}'
        AND row_count={3}
        AND event_time='{4}'; COMMIT;
    """.format(row_cnt_table, schema_name, table, cur_row_cnt, datetime_to_micro_sec, new_obsv_cnt)


def add_new_row_cnt_record(row_cnt_table, schema_name, table_name, cur_row_cnt, obsvn_cnt=0):
    return """
        INSERT INTO {1}.{0}
        VALUES ('{2}', now(), {3}, {4}); COMMIT;
    """.format(row_cnt_table, schema_name, table_name, cur_row_cnt, obsvn_cnt)


def run_query_to_modify_row_cnt_table(cursor, query):
    """
    This wrapper function ensures that we raise proper error alert if adding/modifying rows in row_cnt_table fails.
    """
    cursor.execute(query)
    result = cursor.fetchall()  # if successful, this returns [[1]]

    if (not result) or (result and result[0] and result[0][0] != 1):
        raise ErrorInUpdatingRowCountTableEntry({
            "reason": "ErrorInUpdatingRowCountTableEntry: fetchall did not return expected [[1]].",
            "query_used": query
        })


def trigger_on_row_count_change(table_and_actions, obsvn_cnt=3):
    trigger_script_when_cnt_reach = obsvn_cnt
    schema_name = 'gaintheory_us_targetusa_14'
    row_cnt_table = 'incampaign_row_count'

    try:
        with vertica_python.connect(**conn_info) as connection:
            print("Checking row count...")
            cursor = connection.cursor()
            for table, procedures in table_and_actions.items():
                cur_row_cnt = get_row_count(cursor, table, schema_name)[0]
                if cur_row_cnt < 0:
                    raise RowCountInValid({
                        "reason": "RowCountInValid. Maybe the table doesn't exist yet or something went wrong in counting the rows",
                        "cur_row_cnt": cur_row_cnt, "table": table})
                else:
                    cursor.execute(get_most_recent_row_cnt(row_cnt_table, schema_name, table))
                    result = cursor.fetchall()

                    if not result:
                        # No such table name recorded before. Add it for the first time
                        cursor.execute(add_new_row_cnt_record(row_cnt_table, schema_name, table, cur_row_cnt))
                    else:
                        recorded_row_cnt = result[0][2]

                        if cur_row_cnt == recorded_row_cnt:
                            new_obsvn_cnt = result[0][3] + 1
                            date_time_to_micro_sec = str(result[0][1])
                            q = increase_observation_cnt(row_cnt_table, schema_name, table, cur_row_cnt,
                                                         date_time_to_micro_sec, new_obsvn_cnt)
                            run_query_to_modify_row_cnt_table(cursor, q)

                            if new_obsvn_cnt == trigger_script_when_cnt_reach:
                                for proc in procedures:
                                    try:
                                        cmd = ' '.join([] + proc['cmd'])
                                        print(cmd)
                                        output = subprocess.check_output(cmd,
                                                                         stderr=subprocess.PIPE)
                                        send_dev_email(table, new_obsvn_cnt, cmd, output)

                                        if 'notify_on_complete' in proc:
                                            subject = proc['notify_on_complete']['subject']
                                            body = proc['notify_on_complete']['body']
                                            recipients = proc['notify_on_complete']['recipients']
                                            attachment = None
                                            if 'attachment' in proc['notify_on_complete']:
                                                attachment = proc['notify_on_complete']['attachment']
                                            send_notification_email(recipients, subject, body, attachment)
                                    except Exception as err:
                                        send_error_email(repr(err))
                        else: # Row count for the table has changed. Enter a new entry/row for this.
                            q = add_new_row_cnt_record(row_cnt_table, schema_name, table, cur_row_cnt)
                            run_query_to_modify_row_cnt_table(cursor, q)

    except vertica_python.errors.QueryError as err:
        print("Vertica Query Error!")
        send_error_email(err)
    except (RowCountInValid, ErrorInUpdatingRowCountTableEntry) as err:
        print("Error related to updating or fetching row count of the table!")
        send_error_email(err.args[0])
    except subprocess.CalledProcessError as err:
        print("CalledProcessError. Detail =>", err)
        send_error_email(err)
    except Exception as err:
        print("Unknown Error Occurred!")
        send_error_email(err)


