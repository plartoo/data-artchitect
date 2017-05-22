import subprocess

from account_info import *
from mailer import Mailer


def send_dev_email(cmd_used, status_msg):
    status_msg = str(status_msg).replace('\\r\\n', '<br>')
    subject = "Command triggered: " + cmd_used
    body = """
    <p>The scheduled task is run with the command: {0} </p>
    <p>It finished with the following message: {1}</p>
    """.format(cmd_used, status_msg)
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


def trigger_once(procedures):

    for proc in procedures:
        try:
            cmd = ' '.join([] + proc['cmd'])
            print(cmd)
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT) #stderr=subprocess.PIPE)
            send_dev_email(cmd, output)

            if 'notify_on_complete' in proc:
                subject = proc['notify_on_complete']['subject']
                body = proc['notify_on_complete']['body']
                recipients = proc['notify_on_complete']['recipients']
                attachment = None
                if 'attachment' in proc['notify_on_complete']:
                    attachment = proc['notify_on_complete']['attachment']
                send_notification_email(recipients, subject, body, attachment)
        except subprocess.CalledProcessError as err:
            print("CalledProcessError. Detail =>", err)
            send_error_email(err)
        except Exception as err:
            send_error_email(repr(err))
