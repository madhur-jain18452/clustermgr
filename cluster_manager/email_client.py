"""
Email Client: Wrapper to send the Mails to the users of the system

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import json
import smtplib
from email.mime.multipart import MIMEMultipart

class EmailClient:
    def __init__(self, user_mailing_list=None, email_config_file=None):
        if email_config_file is None:
            self.email_config = {
                "server": {
                    "address": "10.46.18.111",
                    "port": 25,
                    "credentials": {
                        "username": "era@nutanix.com",
                        "password": "Nutanix.1"
                    }
                },
                "cc_list": [] + (user_mailing_list if user_mailing_list else [])
            }
        else:
            with open(email_config_file, "r") as ecf:
                self.email_config = json.loads(ecf.read())
            return

    def send_email(self, to_email):
        smtp_server = smtplib.SMTP(self.email_config['server']['address'],
                                   self.email_config['server']['port'])
        message = MIMEMultipart()
        message['from'] = self.email_config['server']['credentials']['username']
        message['to'] = ','.join([to_email] + self.email_config['cc_list'])
        # TODO PARSE THE MESSAGE
        text = message.as_string()
        smtp_server.sendmail(self.email_config['server']['credentials']['username'],
                             [to_email] + self.email_config['cc_list'],
                             text)
        smtp_server.quit()
