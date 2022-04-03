# import modules
import os
import smtplib

# define functions
def send_email(subject, body):
    """Send email (e.g., a download log).
    
    Parameters:
    subject (str): Subject line for the email.
    body (str): Body of the email.
    """
    
    # load email configuration
    mail_name = os.environ['MAIL_NAME'] # email account the message will be sent from
    mail_pass = os.environ['MAIL_PASS'] # email password for the account the message will be sent from
    mail_to = os.environ['MAIL_TO'] # email the message will be sent to
    mail_sender = (os.environ['MAIL_ALIAS'] if 'MAIL_ALIAS' in os.environ.keys() else os.environ['MAIL_NAME']) # the listed sender of the email (either the mail_name or an alias email)
    smtp_server = os.environ['SMTP_SERVER'] # SMTP server address
    smtp_port = int(os.environ['SMTP_PORT']) # SMTP server port
    
    # compose message
    email_text = """\
From: %s
To: %s
Subject: %s

%s
""" % (mail_sender, mail_to, subject, body)
    
    # send message
    try:
        print('Sending message...')
        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.ehlo()
        server.login(mail_name, mail_pass)
        server.sendmail(mail_sender, mail_to, email_text)
        server.close()
        print('Message sent!')
    except Exception as e:
        print(e)
        print('Message failed to send.')