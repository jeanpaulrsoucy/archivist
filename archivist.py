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

## core utilities
import sys
import os
import argparse
import re
import time
from datetime import datetime, timedelta
import pytz
import tempfile
import csv
import json
from zipfile import ZipFile
import hashlib
from array import *

## other utilities
import pandas as pd
import numpy as np 
from colorit import *

## web scraping
import requests
from selenium import webdriver # requires ChromeDriver and Chromium/Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

## notifications
import smtplib
import http.client
import urllib

## Amazon S3
import boto3

# parse arguments
def arg_parser():
    # initialize parser and add arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices = ["test", "prod", "index"], help = "Run mode: prod, test, index")
    parser.add_argument("path_to_datasets_json", help = "The path to the JSON file containing dataset information")
    parser.add_argument("out_path", help = "Where to write the output file (if any)", nargs='?', default=None)
    parser.add_argument("--uuid", nargs = "+", required = False, help = "Specify UUIDs of individual datasets to download")
    parser.add_argument("--uuid-exclude", nargs = "+", required = False, help = "Download all datasets except the specified datasets (ignored when --uuid is set)")
    parser.add_argument("--email", required = False, action = "store_true", dest = "email", help = "If present, an email will be sent at the end of the run (ignored for test runs with no errors)")
    parser.add_argument("--notify", required = False, action = "store_true", dest = "notify", help = "If present, a Pushover notification will be sent at the end of a prod run (prod only)")
    parser.add_argument("--upload-log", required = False, action = "store_true", dest = "upload_log", help = "If present, the log of the run will be uploaded to the S3 bucket (prod only)")
    parser.add_argument("-d", "--debug", nargs = "+", choices = ["print-md5"], required = False, help = "Optional debug parameters")
    # parse args
    args = parser.parse_args()
    # add empty debug list, if necessary
    if args.debug is None:
        args.debug = []
    # return parsed args
    return args

# common functions
def get_datetime():
    tz = 'America/Toronto'
    t = datetime.now(pytz.timezone(tz))
    return t

# define Archivist class
class Archivist:
    def __init__(self):
        # parse arguments
        args = arg_parser()
        # set attributes
        self.t = get_datetime().strftime("%Y-%m-%d %H:%M") # record start time
        self.options = {
            "mode": args.mode,
            "path_to_datasets_json": args.path_to_datasets_json,
            "out_path": args.out_path,
            "uuid": args.uuid,
            "uuid_exclude": args.uuid_exclude
        }
        self.log = {
            "log": "",
            "success": 0,
            "failure": 0,
            "failure_uuid": []
        }
        self.log_options = {
            "email": True if args.email else False,
            "notify": True if args.notify else False,
            "upload_log": True if args.upload_log else False
        }
        self.debug_options = {
            "print_md5": True if "print-md5" in args.debug else False
        }
        with open(self.options["path_to_datasets_json"]) as json_file:
            self.ds_raw = json.load(json_file)
        self.ds = self.load_ds() # load active datasets for this run
        if (self.options["mode"] != "test"):
            self.s3 = {
                "aws_id": os.environ["AWS_ID"],
                "aws_key": os.environ["AWS_KEY"],
                "bucket_name": os.environ["S3_BUCKET"],
                "bucket_root": os.environ["S3_ROOT"],
                "bucket_url": os.environ["S3_URL"]
            }
            self.s3["bucket"] = self.connect_s3(
                s3_bucket = self.s3["bucket_name"],
                aws_id = self.s3["aws_id"],
                aws_key = self.s3["aws_key"])
        # print run options
        if self.options["mode"] == "prod" or self.options["mode"] == "test":
            if self.log_options["email"]:
                print("An email will be sent at the end of this run.")
            else:
                print("No email will be sent at the end of this run.")
            if self.debug_options["print_md5"]:
                print("DEBUG: MD5 hashes will be printed for each downloaded dataset.")
            if self.options["mode"] == "prod":
                if self.log_options["notify"]:
                    print("A notification will be sent at the end of this run.")
                else:
                    print("No notification will be sent at the end of this run.")
                if self.log_options["upload_log"]:
                    print("A log will be uploaded at the end of this run.")
                else:
                    print("No log will be uploaded at the end of this run.")
        
    # define methods
    def record_success(self, f_name):
        self.log["success"] += 1
        self.log["log"] += 'SUCCESS: ' + f_name + '\n'
        print(background('SUCCESS: ' + f_name, Colors.blue))
    
    def record_failure(self, f_name, uuid):
        self.log["failure"] += 1
        self.log["log"] += 'FAILURE: ' + f_name + '\n'
        self.log["failure_uuid"].append(uuid)
        print(background('FAILURE: ' + f_name, Colors.red))

    def connect_s3(self, s3_bucket, aws_id, aws_key):
        try:
            aws = boto3.Session(
                aws_access_key_id = aws_id,
                aws_secret_access_key = aws_key)
            s3 = aws.resource("s3").Bucket(s3_bucket)
            print("Successfully connected to S3 bucket.")
            return s3
        except Exception as e:
            print(e)
            sys.exit("Failed to connect to S3 bucket.")

    def load_ds(self):
        # load active datasets
        datasets = self.ds_raw["active"]
        # convert datasets into a single dictionary
        ds = {}
        for d in datasets:
            for i in range(len(datasets[d])):
                ds[datasets[d][i]['uuid']] = datasets[d][i]
        # set datasets to be downloaded
        if self.options["uuid"]:
            # ignore --uuid-exclude, if present
            if self.options["uuid_exclude"]:
                print("Ignoring --uuid-exclude, as --uuid is set.")
            # remove duplicates, preserving order
            self.options["uuid"] = list(dict.fromkeys(self.options["uuid"]))
            # print specified UUIDs
            print("Specified datasets:", ", ".join(self.options["uuid"]))
            # remove invalid UUIDs
            invalid = list(set(self.options["uuid"]) - set(list(ds.keys())))
            if len(invalid) > 0:
                for i in invalid:
                    self.options["uuid"].remove(i)
                # report removed UUIDs
                print("Removed invalid UUIDs: " + ", ".join(invalid))
            # subset dataset list
            if len(self.options["uuid"]) > 0:
                ds = {key: ds[key] for key in self.options["uuid"]}
            else:
                sys.exit("No valid UUIDs specified. Exiting.")
        elif self.options["uuid_exclude"]:
            # remove duplicates, preserving order
            self.options["uuid_exclude"] = list(dict.fromkeys(self.options["uuid_exclude"]))
            # print excluded UUIDs
            print("Downloading all datasets except the following: ", ", ".join(self.options["uuid_exclude"]))
            invalid = list(set(self.options["uuid_exclude"]) - set(list(ds.keys())))
            if len(invalid) > 0:
                for i in invalid:
                    self.options["uuid_exclude"].remove(i)
                print("Removed invalid UUIDs from exclusion list: " + ", ".join(invalid))
            # subset dataset list
            if len(self.options["uuid_exclude"]) > 0:
                for i in self.options["uuid_exclude"]:
                    ds.pop(i)
        else:
            if self.options["mode"] != "index":
                print("No datasets specified. Downloading all datasets...")
        # verify dataset list is not empty
        if len(ds) == 0:
            sys.exit("No valid UUIDs specified. Exiting.")
        # return dataset list
        return ds
    
    def print_success_failure(self):
        total_files = str(self.log["success"] + self.log["failure"])
        print(background('Successful downloads: ' + str(self.log["success"]) + '/' + total_files, Colors.blue))
        print(background('Failed downloads: ' + str(self.log["failure"]) + '/' + total_files, Colors.red))

    def generate_rerun_code(self):
        # base code
        code = "python -m archivist " + self.options["mode"] + " " + self.options["path_to_datasets_json"]
        # get options
        if self.log_options["email"]:
            code += " --email"
        if self.log_options["notify"]:
            code += " --notify"
        if self.log_options["upload-log"]:
            code += " --upload-log"
        if self.debug_options["print_md5"]:
            code += " --debug print-md5"
        # add failed UUIDs
        if len(self.log["failure_uuid"]) > 0:
            code += " --uuid " + " ".join(self.log["failure_uuid"])
        else:
            print("No failed UUIDs found. No rerun code will be generated.")
            return None
        # return rerun code
        code = "The following code will rerun failed datasets:\n" + code
        return code

    def output_log(self):
        # process download log: place failures at the top, successes below
        log = self.log["log"]
        success = self.log["success"]
        failure = self.log["failure"]
        log = log.split('\n')
        log.sort()
        log = '\n'.join(log)
        # count total files
        total_files = str(success + failure)
        # assemble log text
        log = 'Successful downloads: ' + str(success) + '/' + total_files + '\n' + 'Failed downloads: ' + str(failure) + '/' + total_files + '\n' + log + '\n'
        if failure > 0:
            log = log + '\n' + self.generate_rerun_code()
        log = self.t + '\n\n' + log
        # return log
        return log
    
    def upload_log(self, log):
        print("Uploading recent log...")
        try:
            # write most recent log entry temporarily and upload
            tmpdir = tempfile.TemporaryDirectory()
            f_path = os.path.join(tmpdir.name, 'log.txt')
            with open(f_path, "w") as local_file:
                local_file.write(log)
            f_key = os.path.join(a.s3["bucket_root"], "log_recent.txt")
            self.s3["bucket"].upload_file(Filename=f_path, Key=f_key)
            # report success
            print(color("Recent log upload successful!", Colors.green))
        except:
            print(background("Recent log upload failed!", Colors.red))
        print("Appending recent log to full log...")
        try:
            # read in full log
            tmpdir = tempfile.TemporaryDirectory()
            d_path = os.path.join(tmpdir.name, "log.txt")
            d_key = os.path.join(a.s3["bucket_root"], "log.txt")
            self.s3["bucket"].download_file(Filename=d_path, Key=d_key)
            with open(d_path, "r") as full_log:
                full_log = full_log.read()
            # append recent log to full log
            log = full_log + '\n\n' + log
            # write log temporarily and upload
            f_path = os.path.join(tmpdir.name, "log.txt")
            f_key = os.path.join(a.s3["bucket_root"], "log.txt")
            with open(f_path, "w") as local_file:
                local_file.write(log)
            self.s3["bucket"].upload_file(Filename=f_path, Key=f_key)
            # report success
            print(color("Full log upload successful!", Colors.green))
        except:
            print(background("Full log upload failed!", Colors.red))
    
    def create_index(self):

        # temporarily disable pandas chained assignment warning
        # otherwise, a sprurious warning will be printed
        pd_option = pd.get_option('chained_assignment') # save previous value
        pd.set_option('chained_assignment', None) # disable
        
        # get dataset list
        datasets = self.ds_raw
        
        # convert datasets into single dictionary
        ds = {}
        for a in datasets: # active and inactive
            for d in datasets[a]:
                for i in range(len(datasets[a][d])):
                    ds[datasets[a][d][i]['uuid']] = datasets[a][d][i]
        
        # prepare paginator for list of all files in the archive
        paginator = boto3.client(
            "s3",
            aws_access_key_id=self.s3["aws_id"],
            aws_secret_access_key=self.s3["aws_key"]).get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.s3["bucket_name"], Prefix=self.s3["bucket_root"])
        
        # create inventory of files in the archive
        inv = []
        for page in pages:
            for obj in page['Contents']:
                inv.append([obj['Key'], obj['Size'], obj['ETag']])
        inv = pd.DataFrame(inv, columns = ['file_path', 'file_size', 'file_etag'])
        
        # calculate other columns
        inv['dir_parent'] = inv['file_path'].apply(lambda x: os.path.dirname(x).split('/')[1:-1])
        inv['dir_parent'] = inv['dir_parent'].apply(lambda x: '/'.join(x))
        inv['dir_file'] = inv['file_path'].apply(lambda x: os.path.dirname(x).split('/')[-1])
        inv['file_name'] = inv['file_path'].apply(lambda x: os.path.basename(x))
        inv['file_timestamp'] = inv['file_name'].str.extract('(?<=_)(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}).*$', expand=True)
        inv['file_date'] = pd.to_datetime(inv['file_timestamp'], format='%Y-%m-%d_%H-%M').dt.date
        inv['file_url'] = self.s3["bucket_url"] + inv['file_path']
        # initialize other columns
        inv['file_date_true'] = inv['file_date'] # set initial values for true date
        inv['file_etag_duplicate'] = np.nan
        # remove directories, log files and supplementary files
        inv = inv[inv['file_name'] != ''] # remove directories
        inv = inv[inv['dir_file'] != self.s3["bucket_root"]] # remove log files (stored in root)
        inv = inv[inv['dir_file'] != 'supplementary'] # remove supplementary files
        # keep only necessary columns and reorder
        inv = inv[['dir_parent', 'dir_file', 'file_name', 'file_timestamp', 'file_date', 'file_date_true', 'file_size', 'file_etag', 'file_etag_duplicate', 'file_url']]
        # sort
        inv = inv.sort_values(by=['dir_parent', 'dir_file', 'file_timestamp'])
        
        # initialize index and add UUID column
        ind = pd.DataFrame(columns=inv.columns)
        ind.insert(0, 'uuid', '')
        
        # calculate true dates and etag duplicates - loop through each dataset
        for key in ds:
            d_p = ds[key]['dir_parent']
            d_f = ds[key]['dir_file']
            # get data
            d = inv[(inv['dir_parent'] == d_p) & (inv['dir_file'] == d_f)]
            # if no data, skip to next dataset
            # (this occurs if a dataset was recently added but has not yet been archived)
            if len(d) == 0:
                continue
            # check if there are multiple hashes on the first date of data
            d_first_date = d[d['file_date'] == d['file_date'].min()].drop_duplicates(['file_etag'])
            if (len(d_first_date) > 1):
                # if there multiple hashes on the first date, assume the earliest file is actually from the previous date
                d.loc[d['file_name'] == d_first_date.iloc[0]['file_name'], 'file_date_true'] = d.loc[d['file_name'] == d_first_date.iloc[0]['file_name'], 'file_date'] - timedelta(days=1)
            # generate list of all possible dates: from first true date to last true date
            d_dates_seq = pd.DataFrame(pd.date_range(d['file_date_true'].min(), d['file_date'].max()))
            d_dates_seq = pd.to_datetime(d_dates_seq[0]).dt.date.tolist()
            # generate list of all dates in the dataset
            d_dates = d['file_date_true'].unique().tolist()
            # are any expected dates are missing?
            d_dates_missing = np.setdiff1d(d_dates_seq, d_dates)
            if (len(d_dates_missing) > 0):
                # if there are any missing dates, check if there are multiple hashes in the following day
                for j in d_dates_missing:
                    d_dates_next = d[d['file_date_true'] == j + timedelta(days=1)].drop_duplicates(['file_etag'])
                    if len(d_dates_next) > 1:
                        # if there are more than 0 or 1 hashes on the previous date, assume the earliest hash actually corresponds to the missing day
                        d.loc[d['file_name'] == d_dates_next.iloc[0]['file_name'], 'file_date_true'] = d.loc[d['file_name'] == d_dates_next.iloc[0]['file_name'], 'file_date_true'] - timedelta(days=1)
            # using true date, keep only the final hash of each date ('definitive file' for that date)
            d = d.drop_duplicates(['file_date_true'], keep='last')
            # using hash, mark duplicates appearing after the first instance (e.g., duplicate hashes of Friday value for weekend versions of files updated only on weekdays)
            d['file_etag_duplicate'] = d['file_etag'].duplicated()
            # mark duplicates using 1 and 0 rather than True and False
            d['file_etag_duplicate'] = np.where(d['file_etag_duplicate']==True, 1, 0)
            # finally, add UUID
            d['uuid'] = key
            # save modified index
            ind = pd.concat([ind, d])
            # print progress
            print(d_p + '/' + d_f)
        
        # reset pandas chained assignment warning option
        pd.set_option('chained_assignment', pd_option) # reset
        
        # return index
        return(ind)

    def write_index(self, ind, out_path):
        if out_path is None:
            print('Uploading file index...')
            try:
                # write file index temporarily and upload to Amazon S3
                tmpdir = tempfile.TemporaryDirectory()
                f_path = os.path.join(tmpdir.name, 'file_index.csv')
                f_key = os.path.join(self.s3["bucket_root"], "file_index.csv")
                ind.to_csv(f_path, index=False)
                a.s3["bucket"].upload_file(Filename=f_path, Key=f_key)
                # report success
                print(color('File index upload successful!', Colors.green))
            except Exception as e:
                # print error message
                print(e)
                # report failure
                print(background('File index upload failed!', Colors.red))
        else:
            print('Writing file index...')
            try:
                # write file index
                ind.to_csv(out_path, index=False)
                # report success
                print(color('File index has been written to: ' + out_path, Colors.green))
            except Exception as e:
                # print error message
                print(e)
                # report failure
                print(background('File index failed to write to: ' + out_path, Colors.red))

# define downloader class
class downloader:
    def __init__(self, uuid):
        # get UUID info
        self.uuid_info = self.get_dataset_info(uuid)
        # begin download
        self.dl_fun(self.uuid_info)
    
    # define methods
    def arg_bool(self, k, v):
        try:
            if v == "True":
                return True
            elif v == "False":
                return False
            else:
                raise ValueError
        except:
            print("Error interpreting arg " + k + ", setting value to False.")
            return False

    def arg_int(self, k, v):
        try:
            return(int(v))
        except:
            print("Error interpreting arg " + k + ", setting value to 0.")
            return 0
    
    def get_dataset_info(self, uuid):
        d = a.ds[uuid]
        uuid_info = {"uuid": uuid}
        # verify dataset is active
        if d["active"] != "True":
            raise Exception(uuid + ": Dataset is marked as inactive, skipping...")
        # get URL
        if "url" in d:
            uuid_info["url"] = d["url"]
        elif "url_fun_python" in d:
            # try to run URL function
            try:
                # create local namespace
                loc = {}
                # execute url_fun_python, which returns the URL as url_current in the local namespace
                exec(d['url_fun_python'], {}, loc)
                uuid_info["url"] = loc["url_current"]
                print(uuid + ", retrieved URL: " + uuid_info["url"]) # print result
            except Exception as e:
                # print error message
                print(e)
                # report failure
                print(background(uuid + ": Failed to retrieve URL", Colors.red))
        else:
            raise Exception(uuid + ": Neither a URL nor a URL function are given, skipping...")
        # get ID name and report
        id_name = d["id_name"]
        uuid_info["id_name"] = id_name
        print(id_name)
        # get file name, path and extension
        uuid_info["file_name"] = d["file_name"]
        uuid_info["file_path"] = os.path.join(d["dir_parent"], d["dir_file"], uuid_info["file_name"])
        uuid_info["file_ext"] = "." + d["file_ext"]
        # process other arguments
        uuid_info["args"] = {}
        # process bool args
        bool_args = [
            "user", "rand_url", "verify",
            "unzip", "js",
            "ab_json_to_csv", "mb_json_to_csv"
            ]
        for k, v in d["args"].items():
            if k in bool_args:
                uuid_info["args"][k] = self.arg_bool(k, v)
        # process int args
        int_args = [
            "wait", "width", "height"
            ]
        for k, v in d["args"].items():
            if k in int_args:
                uuid_info["args"][k] = self.arg_int(k, v)
        # download function
        uuid_info["dl_fun"] = d["dl_fun"]
        # use dl_file instead of html_page for simple HTML pages (pages not requiring JS)
        if uuid_info["dl_fun"] == "html_page":
            if "js" in uuid_info["args"]:
                if uuid_info["args"]["js"] is False:
                    uuid_info["dl_fun"] = "dl_file"
                    uuid_info["args"].pop("js", None) # dl_file will not accept this arg
                else:
                    pass
            else:
                uuid_info["dl_fun"] = "dl_file"
        # filter out unwanted keywords
        if uuid_info["dl_fun"] == "html_page":
            uuid_info["args"].pop("verify", None) # html_page will not accept this arg
        # return processed dataset information
        return uuid_info
    
    def print_md5(self, f_content):
        try:
            print("md5: " + hashlib.md5(f_content).hexdigest())
        except Exception as e:
            # print error message
            print(e)
            # print failure to produce hash
            print("md5: failed to hash dataset")
    def upload_file(self, f_name, f_path, uuid):
        # generate full S3 key
        f_key = os.path.join(a.s3["bucket_root"], f_name)
        # upload file to S3
        try:
            # upload file
            a.s3["bucket"].upload_file(Filename=f_path, Key=f_key)
            # record success
            a.record_success(f_name)
        except Exception as e:
            # print error message
            print(e)
            # record failure
            a.record_failure(f_name, uuid)
    
    def dl_fun(self, uuid_info):
        # get download function
        dl_fun = uuid_info["dl_fun"]
        # pass info to appropriate download function
        getattr(self, dl_fun)(uuid_info)

    def dl_file(self, uuid_info):
        # set UUID and URL
        uuid = uuid_info["uuid"]
        url = uuid_info["url"]

        # set name with timestamp and file ext
        f_name = uuid_info["file_path"] + '_' + get_datetime().strftime('%Y-%m-%d_%H-%M') + uuid_info["file_ext"]

        # set default parameters
        html = True if uuid_info["file_ext"] == ".html" else False
        verify = uuid_info["args"]["verify"] if "verify" in uuid_info["args"] else True
        user = uuid_info["args"]["user"] if "user" in uuid_info["args"] else False
        rand_url = uuid_info["args"]["rand_url"] if "rand_url" in uuid_info["args"] else False
        unzip = uuid_info["args"]["unzip"] if "unzip" in uuid_info["args"] else False
        ab_json_to_csv = uuid_info["args"]["ab_json_to_csv"] if "ab_json_to_csv" in uuid_info["args"] else False
        mb_json_to_csv = uuid_info["args"]["mb_json_to_csv"] if "mb_json_to_csv" in uuid_info["args"] else False

        # temporary file name
        tmpdir = tempfile.TemporaryDirectory()
        f_path = os.path.join(tmpdir.name, uuid_info["file_name"] + uuid_info["file_ext"])

        # download file
        try:
            # add no-cache headers
            headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"}

            # set normal-looking user agent string, if user is set to True
            # some websites will reject a request unless it appears to be a normal web browser
            if user is True:
                headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101 Firefox/66.0"

            # add random number to url to prevent caching, if requested
            if rand_url is True:
                url = url + "?randNum=" + str(int(datetime.now().timestamp()))

            # request URL
            req = requests.get(url, headers=headers, verify=verify)

            ## check if request was successful
            if not req.ok:
                # record failure
                a.record_failure(f_name, uuid)
            else:
                # DEBUG: print md5 hash of dataset
                if a.debug_options["print_md5"]:
                    if html:
                        self.print_md5(req.text.encode("utf-8"))
                    else:
                        self.print_md5(req.content)
                # successful request: if mode == test, print success and end
                if a.options["mode"] == "test":
                    # record success
                    a.record_success(f_name)
                # successful request: mode == prod, upload file
                else:
                    # special processing
                    if unzip:
                        # unzip data
                        z_path = os.path.join(tmpdir.name, "zip_file.zip")
                        with open(z_path, mode="wb") as local_file:
                            local_file.write(req.content)                        
                        with ZipFile(z_path, "r") as zip_file:
                            zip_file.extractall(tmpdir.name)
                        if uuid == "7a1c4441-b27c-4b3d-a9b6-71a9c24da95d":
                            # read CSV (informative columns only)
                            data = pd.read_csv(f_path, usecols=['REF_DATE', 'Case identifier number', 'Case information', 'VALUE'])
                            # save original order of column values
                            col_order = data['Case information'].unique()
                            # pivot long to wide
                            data = data.pivot(index=['REF_DATE', 'Case identifier number'], columns='Case information', values='VALUE').reset_index()
                            # use original column order
                            data = data[['REF_DATE', 'Case identifier number'] + col_order.tolist()]
                            # write CSV
                            data.to_csv(f_path, index=None, quoting=csv.QUOTE_NONNUMERIC)
                    elif ab_json_to_csv:
                        data = re.search("(?<=\"data\"\:)\[\[.*\]\]", req.text).group(0)
                        if url == "https://www.alberta.ca/maps/covid-19-status-map.htm":
                            data = BeautifulSoup(data, features="html.parser")
                            data = data.get_text() # strip HTML tags
                            # this regex may need some tweaking if measures column changes in the future
                            data = re.sub("<\\\/a><\\\/li><\\\/ul>", "", data) # strip remaining tags
                            data = re.sub("(?<=\") ", "", data) # strip whitespace
                            data = re.sub(" (?=\")", "", data) # strip whitespace
                            data = pd.read_json(data).transpose()
                            data = data.rename(columns={0: "", 1: "Region name", 2: "Measures", 3: "Active case rate (per 100,000 population)", 4: "Active cases", 5: "Population"})
                        elif url == "https://www.alberta.ca/schools/covid-19-school-status-map.htm":
                            data = re.sub(',"container":.*', "", data) # strip remaining tags
                            data = pd.read_json(data).transpose()
                            data = data.rename(columns={0: "", 1: "Region name", 2: "School status", 3: "Schools details", 4: "num_ord"})
                            data['num_ord'] = data['num_ord'].astype(str).astype(int) # convert to int
                            data[''] = data[''].astype(str).astype(int) # convert to int
                            data = data.sort_values(by=['num_ord', '']) # sort ascending by num_ord and first column (like CSV output on website)
                        data = data.to_csv(None, quoting=csv.QUOTE_ALL, index=False) # to match website output: quote all lines, don't terminate with new line
                        with open(f_path, 'w') as local_file:
                            local_file.write(data[:-1])
                    elif mb_json_to_csv:
                        # for Manitoba JSON data only: convert JSON to CSV and save as temporary file                    
                        data = pd.json_normalize(json.loads(req.content)['features'])
                        data.columns = data.columns.str.lstrip('attributes.') # strip prefix
                        # replace timestamps with actual dates
                        if 'Date' in data.columns:
                            data.Date = pd.to_datetime(data.Date / 1000, unit='s').dt.date
                        data.to_csv(f_path, index=None)
                    else:
                        # all other data: write contents to temporary file
                        with open(f_path, mode="wb") as local_file:
                            local_file.write(req.content)
                    # upload file
                    self.upload_file(f_name, f_path, uuid)
        except Exception as e:
            # print error message
            print(e)
            # record failure
            a.record_failure(f_name, uuid)
    
    def load_webdriver(self, tmpdir, user=False):
        """Load Chromium headless webdriver for Selenium.

        Parameters:
        tmpdir (TemporaryDirectory): A temporary directory for saving files downloaded by the headless browser.
        user (bool): Should the request impersonate a normal browser? Needed to access some data. Default: False.
        """
        options = Options()
        options.binary_location = os.environ['CHROME_BIN']
        options.add_argument("--headless")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        prefs = {'download.default_directory' : tmpdir.name}
        options.add_experimental_option('prefs', prefs)
        if user:
            options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101 Firefox/66.0")
        driver = webdriver.Chrome(executable_path=os.environ['CHROMEDRIVER_BIN'], options=options)
        return driver

    def click_xpath(self, driver, wait, xpath):
        element = WebDriverWait(driver, timeout=wait).until(
            EC.element_to_be_clickable((By.XPATH, xpath)))
        element.click()
        return driver

    def click_linktext(self, driver, wait, text):
        element = WebDriverWait(driver, timeout=wait).until(
            EC.element_to_be_clickable((By.LINK_TEXT, text)))
        element.click()
        return driver

    def html_page(self, uuid_info):

        # set UUID and URL
        uuid = uuid_info["uuid"]
        url = uuid_info["url"]

        # set name with timestamp and file ext
        f_name = uuid_info["file_path"] + '_' + get_datetime().strftime('%Y-%m-%d_%H-%M') + uuid_info["file_ext"]
        
        # temporary file name
        tmpdir = tempfile.TemporaryDirectory()
        f_path = os.path.join(tmpdir.name, uuid_info["file_name"] + uuid_info["file_ext"])

        # set default parameters
        user = uuid_info["args"]["user"] if "user" in uuid_info["args"] else False
        wait = uuid_info["args"]["wait"] if "wait" in uuid_info["args"] else 0

        # download file
        try:
            # load webdriver
            driver = self.load_webdriver(tmpdir, user=user)
            # load page
            driver.get(url)
            # special processing
            try:
                if uuid == '9ed0f5cd-2c45-40a1-94c9-25b0c9df8f48':
                    # show other figure in tabset
                    time.sleep(wait) # allow first figure to load
                    driver = self.click_linktext(driver, wait, 'Tests by Specimen Collection Date') # ID is dynamic
                elif uuid == '8814f932-33ec-49ef-896d-d1779b2abea7':
                    # wait for tab link to be clickable then click
                    driver = self.click_xpath(driver, wait, '/html/body/div[1]/nav/div/ul/li[1]/a')
                    driver = self.click_xpath(driver, wait, '/html/body/div[1]/nav/div/ul/li[1]/ul/li[2]/a')
                    # time.sleep(wait); driver.find_element_by_id('complete').get_attribute('innerHTML') # test
                elif uuid == '391d177d-1ea8-45ac-bca4-d9f86733c253':
                    # wait for tab link to be clickable then click
                    driver = self.click_xpath(driver, wait, '/html/body/div[1]/nav/div/ul/li[2]/a')
                    # time.sleep(wait); driver.find_element_by_id('Title2').get_attribute('innerHTML') # test
                elif uuid == 'effdfd82-7c59-4f49-8445-f1f8f73b6dc2':
                    # wait for tab link to be clickable then click
                    driver = self.click_xpath(driver, wait, '/html/body/div[1]/nav/div/ul/li[3]/a')
                    driver = self.click_xpath(driver, wait, '/html/body/div[1]/nav/div/ul/li[3]/ul/li[1]/a')
                    # whole population coverage
                    driver = self.click_xpath(driver, wait, '//*[@id="all"]')
                    # show all data tables
                    elements = driver.find_elements_by_link_text('Data Table')
                    for element in elements:
                        element.click()
                    # time.sleep(wait); driver.find_element_by_id('VCTitle2').get_attribute('innerHTML') # test
                elif uuid == '454de458-f7b4-4814-96a6-5a426f8c8c60':
                    # wait for tab link to be clickable then click
                    driver = self.click_xpath(driver, wait, '/html/body/div[1]/nav/div/ul/li[3]/a')
                    driver = self.click_xpath(driver, wait, '/html/body/div[1]/nav/div/ul/li[3]/ul/li[2]/a')
                    # time.sleep(wait); driver.find_element_by_id('VCTitle').get_attribute('innerHTML') # test
                elif uuid == 'b32a2f6b-7745-4bb1-9f9b-7ad0000d98a0':
                    # wait for tab link to be clickable then click
                    driver = self.click_xpath(driver, wait, '/html/body/div[1]/report-embed/div/div/div[1]/div/div/div/exploration-container/div/div/div/exploration-host/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container[4]/transform/div/div[3]/div/visual-modern')
                elif uuid == 'e00e2148-b0ea-458b-9f00-3533e0c5ae8e':
                    # wait for tab link to be clickable then click
                    driver = self.click_xpath(driver, wait, '/html/body/div[1]/report-embed/div/div/div[1]/div/div/div/exploration-container/div/div/div/exploration-host/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container-group[2]/transform/div/div[2]/visual-container[3]/transform/div/div[3]/div/visual-modern')
            # print error message
            except Exception as e:
                print(e)
            
            # save HTML of webpage
            time.sleep(wait) # complete page load
            page_source = driver.page_source
            # DEBUG: print md5 hash of dataset
            if a.debug_options["print_md5"]:
                self.print_md5(page_source.encode("utf-8"))
            # save HTML file
            with open(f_path, "w") as local_file:
                local_file.write(page_source)

            # verify download
            if not os.path.isfile(f_path):
                # record failure
                a.record_failure(f_name, uuid)
            # successful request: if mode == test, print success and end
            elif a.options["mode"] == "test":
                # record success
                a.record_success(f_name)
            # successful request: mode == prod, prepare files for data upload
            else:
                # upload file
                self.upload_file(f_name, f_path, uuid)
            # quit webdriver
            driver.quit()
        except Exception as e:
            # print error message
            print(e)
            # record failure
            a.record_failure(f_name, uuid)

    def ss_page(self, uuid_info):

        # set UUID and URL
        uuid = uuid_info["uuid"]
        url = uuid_info["url"]

        # set name with timestamp and file ext
        f_name = uuid_info["file_path"] + '_' + get_datetime().strftime('%Y-%m-%d_%H-%M') + uuid_info["file_ext"]
        
        # temporary file name
        tmpdir = tempfile.TemporaryDirectory()
        f_path = os.path.join(tmpdir.name, uuid_info["file_name"] + uuid_info["file_ext"])

        # set default parameters
        user = uuid_info["args"]["user"] if "user" in uuid_info["args"] else False
        wait = uuid_info["args"]["wait"] if "wait" in uuid_info["args"] else 0
        width = uuid_info["args"]["width"] if "width" in uuid_info["args"] else None
        height = uuid_info["args"]["height"] if "height" in uuid_info["args"] else None

        # download file
        try:
            # load webdriver
            driver = self.load_webdriver(tmpdir, user)
            # load page and wait
            driver.get(url)
            time.sleep(wait) # wait for page to load      
            # get total width of the page if width is not set by user
            if width is None:
                width = driver.execute_script('return document.body.parentNode.scrollWidth')
            # get total height of the page if height is not set by user
            if height is None:
                height = driver.execute_script('return document.body.parentNode.scrollHeight')
            # set window size
            driver.set_window_size(width, height)
            # take screenshot
            try:
                driver.find_element_by_tag_name('body').screenshot(f_path) # remove scrollbar
                # verify screenshot
                if not os.path.isfile(f_path):
                    # record failure
                    a.record_failure(f_name, uuid)
                else:
                    # DEBUG: print md5 hash of dataset
                    if a.debug_options["print_md5"]:
                        self.print_md5(open(f_path, "rb").read())
                    if a.options["mode"] == "test":
                        # record success
                        a.record_success(f_name)
                    else:
                        # upload file
                        self.upload_file(f_name, f_path, uuid)
            except Exception as e:
                # print error message
                print(e)
                # record failure
                a.record_failure(f_name, uuid)
            # quit webdriver
            driver.quit()
        except Exception as e:
            # print error message
            print(e)
            # record failure
            a.record_failure(f_name, uuid)

# notification functions
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

def pushover(message, priority=0, title=None, device=None):
    """Send notification to device via the Pushover API (https://pushover.net/api).
    
    Parameters:
    message (str): The body of the noficiation.
    priority (int): Optional. Message priority, an integer from -2 to 2, see: https://pushover.net/api#priority). Defaults to 0 (normal priority).
    title (str): Optional. The title of the notification. If None (the default), the application's name will be used.
    device (str): Optional. The name of the device to send the notification to. If None (the default), the notification will be sent to all devices.
    """

    # load Pushover configuration
    app_token = os.environ['PO_TOKEN']
    user_key = os.environ['PO_KEY']

    # assemble body
    body = {
        'token': app_token,
        'user': user_key,
        'message': message,
        'priority': priority,
        'title': title,
        'device': device
    }

    # remove unused parameters
    if (title is None):
        body.pop('title')
    if (device is None):
        body.pop('device')
    
    # encode body
    body_enc = urllib.parse.urlencode(body)

    # send notification
    conn = http.client.HTTPSConnection('api.pushover.net:443')
    conn.request('POST', '/1/messages.json', body_enc, { 'Content-type': 'application/x-www-form-urlencoded' })
    status = conn.getresponse().status

    # check response
    if (status == 200):
        print('Notification sent successfully.')
    else:
        print('Status: ' + str(status))
        print('Notification did not send successfully.')
    
    # close connection
    conn.close()

# run module as a script
if __name__ == '__main__':
    init_colorit() # enable printing with colour
    # create Archivist object
    a = Archivist()
    # run
    if a.options["mode"] == "prod" or a.options["mode"] == "test":
        # announce beginning of file downloads
        print('Beginning file downloads...')
        # loop through datasets
        for uuid in a.ds:
            downloader(uuid)
        # summarize successes and failures
        a.print_success_failure()
        # print rerun code, if necessary
        if a.log["failure"] > 0:
                print(background("\n" + a.generate_rerun_code(), (150, 150, 150)))
                print("") # newline
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
                subject = " ".join(["PROD", "Covid19CanadaArchive Log", a.t + ",", "Failed:", str(a.log["failure"])])
                body = log
                # email log
                send_email(subject, body)
            # send pushover notification
            if a.log_options["notify"]:
                # compose notification
                notif = "Success: " + str(a.log["success"]) + "\nFailure: " + str(a.log["failure"])
                pushover(notif, priority=1, title = "Archive update completed")
        else:
            # email log (if there are any failures)
            if a.log_options["email"]:
                if a.log["failure"] > 0:
                    # compose email message
                    subject = " ".join(["TEST", "Covid19CanadaArchive Log", a.t + ",", "Failed:", str(a.log["failure"])])
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
else:
    raise Exception("Module 'archivist' cannot be imported, please run using: python -m archivist")
