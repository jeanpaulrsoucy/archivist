# archivist: Python-based digital archive tool currently powering the Canadian COVID-19 Data Archive #
# https://github.com/jeanpaulrsoucy/archivist #
# Maintainer: Jean-Paul R. Soucy #

# environmental variables required for this module

# common variables (prod / test only)
## CHROME_BIN: path to Chromium/Chrome binary
## CHROMEDRIVER_BIN: path to Chromedriver binary

# S3 variables (prod / index only)
## AWS_ID: AWS ID (i.e., aws_access_key_id)
## AWS_KEY: AWS key (i.e., aws secret access key)
## S3_BUCKET: S3 bucket name
## S3_ROOT: S3 root directory (e.g., dir/subdir)
## S3_URL: base URL for bucket (e.g., https://s3.us-east-2.amazonaws.com/<bucket>/)

# email variables (if --email) (prod / test only)
## MAIL_NAME: email account
## MAIL_PASS: email password
## MAIL_TO: account receiving email logs
## MAIL_ALIAS: (optional) alias email sender name
## SMTP_SERVER: email server address
## SMTP_PORT: email server port

# pushover notification variables (if --notify) (prod only)
## PO_TOKEN: Pushover application token
## PO_KEY: Pushover application key

# import modules
import sys
import os
import tempfile
from colorit import *

# enable colour printing
init_colorit()

# import classes
from archivist.classes.Archivist import Archivist as a
from archivist.classes.Downloader import Downloader

# import functions
from archivist.messenger.email import send_email
from archivist.messenger.pushover import pushover
from archivist.utils.common import get_datetime

# run module as script
a.t = get_datetime().strftime("%Y-%m-%d %H:%M:%S %Z") # record start time
print("Start time: " + a.t) # announce start time
if a.options["mode"] == "prod" or a.options["mode"] == "test":
    # announce beginning of file downloads
    print('Beginning file downloads...')
    # loop through datasets
    for uuid in a.ds:
        Downloader(uuid)
    # summarize successes and failures
    a.print_success_failure()
    # print rerun code, if necessary
    if a.log["failure"] > 0:
        # print names of failed datasets
        print("\nFailed datasets:")
        a.print_failed_uuids()
        # print to stderr
        print("\nThe following code will rerun failed datasets:")
        print(a.generate_rerun_code(), file=sys.stderr)
    # assemble log
    log = a.output_log()
    if a.options["mode"] == "prod":
        # upload log
        if a.log_options["upload_log"]:
            # update update_time.txt in the root directory
            print("Updating update_time.txt...")
            update_time = get_datetime().strftime("%Y-%m-%d %H:%M %Z")
            tmpdir = tempfile.TemporaryDirectory()
            update_time_txt = os.path.join(tmpdir.name, "update_time.txt")
            with open(update_time_txt, "w") as local_file:
                local_file.write(update_time)
            a.s3["bucket"].upload_file(Filename=update_time_txt, Key=os.path.join(a.s3["bucket_root"], "update_time.txt"))
            # upload log
            a.upload_log(log)
        # send email
        if a.log_options["email"]:
            # compose email message
            subject = " ".join(["PROD", a.config["project"]["title"] ,"Log", a.t + ",", "Failed:", str(a.log["failure"])])
            body = log
            # email log
            send_email(subject, body)
        # send pushover notification
        if a.log_options["notify"]:
            # compose notification
            notif = "Success: " + str(a.log["success"]) + "\nFailure: " + str(a.log["failure"])
            pushover(notif, priority=1, title = a.config["project"]["title"] + " update completed")
    else:
        # email log (if there are any failures)
        if a.log_options["email"]:
            if a.log["failure"] > 0:
                # compose email message
                subject = " ".join(["TEST", a.config["project"]["title"] , "Log", a.t + ",", "Failed:", str(a.log["failure"])])
                body = log
                send_email(subject, body)
            else:
                # inform user that log will not be sent as there were no errors
                print("No errors detected during test run. Log will not be sent.")
elif a.options["mode"] == "index":
    ind = a.create_index()
    a.write_index(ind, out_path=a.options["out_path"])
else:
    sys.exit("Please select a valid run mode.")