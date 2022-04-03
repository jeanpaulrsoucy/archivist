# import modules
import argparse
import os
import json
import tempfile
from datetime import timedelta
from colorit import *
import pandas as pd
import numpy as np 
import boto3

# import functions
from archivist.utils.common import get_datetime

# parse arguments
def arg_parser():
    # initialize parser and add arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices = ["test", "prod", "index"], help = "Run mode: prod, test, index")
    parser.add_argument("project_dir", nargs = "?", default = os.getcwd(), help = "Path to the project directory (defaults to the working directory)")
    parser.add_argument("-o", "--out_path", nargs = None, required = False, help = "Where to write the output file (if any)")
    parser.add_argument("-u", "--uuid", nargs = "+", required = False, help = "Specify UUIDs of individual datasets to download")
    parser.add_argument("-x", "--uuid-exclude", nargs = "+", required = False, help = "Download all datasets except the specified datasets (ignored when --uuid is set)")
    parser.add_argument("-m", "--email", required = False, action = "store_true", dest = "email", help = "If present, an email will be sent at the end of the run (ignored for test runs with no errors)")
    parser.add_argument("-n", "--notify", required = False, action = "store_true", dest = "notify", help = "If present, a Pushover notification will be sent at the end of a prod run (prod only)")
    parser.add_argument("-l", "--upload-log", required = False, action = "store_true", dest = "upload_log", help = "If present, the log of the run will be uploaded to the S3 bucket (prod only)")
    parser.add_argument("-d", "--debug", nargs = "+", choices = ["print-md5"], required = False, help = "Optional debug parameters")
    # parse args
    args = parser.parse_args()
    # add empty debug list, if necessary
    if args.debug is None:
        args.debug = []
    # return parsed args
    return args

# define Archivist class
class Archivist:
    def __init__(self):
        # parse arguments
        args = arg_parser()
        # set attributes
        self.t = get_datetime().strftime("%Y-%m-%d %H:%M") # record start time
        self.options = {
            "mode": args.mode,
            "project_dir": args.project_dir,
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
        with open(os.path.join(self.options["project_dir"], "datasets.json")) as json_file:
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
        code = "python -m archivist " + self.options["mode"] + " " + self.options["project_dir"]
        # get options
        if self.log_options["email"]:
            code += " --email"
        if self.log_options["notify"]:
            code += " --notify"
        if self.log_options["upload_log"]:
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
            f_key = os.path.join(self.s3["bucket_root"], "log_recent.txt")
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
            d_key = os.path.join(self.s3["bucket_root"], "log.txt")
            self.s3["bucket"].download_file(Filename=d_path, Key=d_key)
            with open(d_path, "r") as full_log:
                full_log = full_log.read()
            # append recent log to full log
            log = full_log + '\n\n' + log
            # write log temporarily and upload
            f_path = os.path.join(tmpdir.name, "log.txt")
            f_key = os.path.join(self.s3["bucket_root"], "log.txt")
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
                self.s3["bucket"].upload_file(Filename=f_path, Key=f_key)
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

# create Archivist object
Archivist = Archivist()