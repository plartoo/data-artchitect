from account_info import *
from vertica_utils import *
from mailer import Mailer

import subprocess


def send_dev_email(flag_name, trigger_val, cmd_used, status_msg):
    status_msg = str(status_msg).replace('\\r\\n', '<br>')
    subject = "The flag value has changed for: " + flag_name
    body = """
    <p>Because the flag value for flag_name {0} has changed to {1},</p>
    <p>we ran: {2} </p><br>
    <p>That finished with the following message: {3}</p>
    """.format(flag_name, trigger_val, cmd_used, status_msg)
    Mailer().send_email(ADMIN_EMAIL_RECIPIENTS, subject, body)
    print("Admin notification email sent.")


def send_completion_email(recipients, subject, body):
    Mailer().send_email(recipients, subject, body)
    print("Completion email sent.")


def send_error_email(error_msg):
    error_msg = str(error_msg).replace('\\r\\n', '<br>')
    subject = "ERROR in trigger_on_flag_vaule_change.py"
    body = """
    <p>trigger_on_flag_vaule_change.py has run into error below:</p>
    <p><strong style="color: red;">{0}</strong></p>
    """.format(error_msg)
    Mailer().send_email(ERROR_EMAIL_RECIPIENTS, subject, body)
    print("Error notification email sent")


def get_cur_flag_value(table_name, schema_name, flag_name):
    return """
    SELECT run
    FROM {1}.{0}
    WHERE process_name = '{2}'
    """.format(table_name, schema_name, flag_name)


def set_flag_value(table_name, schema_name, flag_name, value):
    return """
    UPDATE {1}.{0}
    SET run = {3}
    WHERE process_name = '{2}';
    COMMIT;
    """.format(table_name, schema_name, flag_name, value)


def trigger_on_flag_value_change(flag_name_and_actions):
    flag_name_and_actions = {
        'fb_etl_and_export_to_s3':
            [
                {'cmd': ['python', ROOT_FOLDER + 'run_vsql.py', SQL_SCRIPT_FOLDER + 'fb_etl.sql']},
                # {'cmd': ['python', ROOT_FOLDER + 'archive_files.py', 'FilesForDatamart/Facebook/',
                #          S3_ARCHIVE_ROOT + 'Facebook/']},
                # {'cmd': ['python', ROOT_FOLDER + 'run_vsql_and_export_to_s3.py',
                #          SQL_SCRIPT_FOLDER + 'export_fb.sql',
                #          'Facebook/', 'FilesForDatamart/Facebook/', 'fb']}
            ]
    }

    DEFAULT_FLAG_VALUE = 0
    trigger_script_when_flag_value = 1
    schema_name = 'gaintheory_us_targetusa_14'
    switch_table = 'incampaign_process_switches'

    try:
        with vertica_python.connect(**conn_info) as connection:
            print("Checking flags...")
            cursor = connection.cursor()
            for flag, procedures in flag_name_and_actions.items():
                cursor.execute(get_cur_flag_value(switch_table, schema_name, flag))
                result = cursor.fetchall()

                if result:
                    cur_flag_val = result[0][0]

                    if cur_flag_val == trigger_script_when_flag_value:
                        for proc in procedures:
                            try:
                                cmd = ' '.join([] + proc['cmd'])
                                print(cmd)
                                output = subprocess.check_output(cmd,
                                                                 stderr=subprocess.PIPE)
                                send_dev_email(flag, trigger_script_when_flag_value, cmd, output)

                                if 'send_msg_on_complete' in proc:
                                    subject = proc['send_msg_on_complete']['subject']
                                    body = proc['send_msg_on_complete']['body']
                                    recipients = proc['send_msg_on_complete']['recipients']
                                    send_completion_email(recipients, subject, body)
                                cmd = ' '.join([] + proc['cmd'])
                                print(cmd)
                                output = subprocess.check_output(cmd,
                                                                 stderr=subprocess.PIPE)
                                send_dev_email(flag, trigger_script_when_flag_value, cmd, output)

                                if 'send_msg_on_complete' in proc:
                                    subject = proc['send_msg_on_complete']['subject']
                                    body = proc['send_msg_on_complete']['body']
                                    recipients = proc['send_msg_on_complete']['recipients']
                                    send_completion_email(recipients, subject, body)
                                cursor.execute(set_flag_value(switch_table, schema_name, flag, DEFAULT_FLAG_VALUE))
                            except Exception as err:
                                send_error_email(err)

    except vertica_python.errors.QueryError as err:
        print("Vertica Query Error!")
        send_error_email(err)
    except subprocess.CalledProcessError as err:
        print("CalledProcessError. Detail =>", err)
        send_error_email(err)
    except Exception as err:
        print("Unknown Error Occurred!")
        send_error_email(err)

if __name__ == "__main__":
    flag_name_and_actions = {
        'flag_name': []
    }
    trigger_on_flag_value_change(flag_name_and_actions)

