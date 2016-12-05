import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from account_info import *

SEPARATOR = ', '

class Mailer:
    def __init__(self):
        self.gmail_accnt = GMAIL_ACCNT
        self.pwd = GMAIL_PWD

    def send_email(self, recipients, subject, body):
        sender = self.gmail_accnt # FROM
        pwd = self.pwd
        to = SEPARATOR.join(recipients) if type(recipients) is list else recipients

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = to
        msg.attach(MIMEText(body, 'html'))

        try:
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.ehlo()
            server.starttls()
            server.login(sender, pwd)
			# recipients needs to be a LIST whereas msg['To'] needs to be a string
			# http://stackoverflow.com/a/28203862
            server.sendmail(sender, recipients, msg.as_string())
            server.close()
        except Exception as err:
            print("Failed to send the email with this error:\n", err)
