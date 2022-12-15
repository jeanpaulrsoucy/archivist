# import modules
import argparse
import os
import json
import toml
import random
import tempfile
from colorit import *
import pandas as pd
import boto3
import re
import time
import hashlib
import sqlite_utils

# parse arguments
def arg_parser():
    # initialize parser and add arguments
    parser = argparse.ArgumentParser()
    # add subparsers
    subparsers = parser.add_subparsers(dest="mode")
    # subparser for mode "prod"
    parser_prod = subparsers.add_parser("prod")
    parser_prod.add_argument("project_dir", nargs = "?", default = os.getcwd(), help = "Path to the project directory (defaults to the working directory)")
    parser_prod.add_argument("-u", "--uuid", nargs = "+", required = False, help = "Specify UUIDs of individual datasets to download")
    parser_prod.add_argument("-x", "--uuid-exclude", nargs = "+", required = False, help = "Download all datasets except the specified datasets (ignored when --uuid is set)")
    parser_prod.add_argument("-m", "--email", required = False, action = "store_true", dest = "email", help = "If present, an email will be sent at the end of the run (ignored for test runs with no errors)")
    parser_prod.add_argument("-n", "--notify", required = False, action = "store_true", dest = "notify", help = "If present, a Pushover notification will be sent at the end of a prod run (prod only)")
    parser_prod.add_argument("-l", "--upload-log", required = False, action = "store_true", dest = "upload_log", help = "If present, the log of the run will be uploaded to the S3 bucket (prod only)")
    parser_prod.add_argument("-i", "--allow-inactive", required = False, action = "store_true", dest = "allow_inactive", help = "If present, datasets marked as inactive will not be skipped")
    parser_prod.add_argument("-r", "--random-order", required = False, action = "store_true", dest = "random_order", help = "If present, datasets will be downloaded in a random order")
    parser_prod.add_argument("-d", "--debug", nargs = "+", choices = ["print-md5", "ignore-ssl"], required = False, help = "Optional debug parameters")
    # subparser for mode "test"
    parser_test = subparsers.add_parser("test")
    parser_test.add_argument("project_dir", nargs = "?", default = os.getcwd(), help = "Path to the project directory (defaults to the working directory)")
    parser_test.add_argument("-u", "--uuid", nargs = "+", required = False, help = "Specify UUIDs of individual datasets to download")
    parser_test.add_argument("-x", "--uuid-exclude", nargs = "+", required = False, help = "Download all datasets except the specified datasets (ignored when --uuid is set)")
    parser_test.add_argument("-m", "--email", required = False, action = "store_true", dest = "email", help = "If present, an email will be sent at the end of the run (ignored for test runs with no errors)")
    parser_test.add_argument("-n", "--notify", required = False, action = "store_true", dest = "notify", help = "If present, a Pushover notification will be sent at the end of a prod run (prod only)")
    parser_test.add_argument("-l", "--upload-log", required = False, action = "store_true", dest = "upload_log", help = "If present, the log of the run will be uploaded to the S3 bucket (prod only)")
    parser_test.add_argument("-i", "--allow-inactive", required = False, action = "store_true", dest = "allow_inactive", help = "If present, datasets marked as inactive will not be skipped")
    parser_test.add_argument("-r", "--random-order", required = False, action = "store_true", dest = "random_order", help = "If present, datasets will be downloaded in a random order")
    parser_test.add_argument("-d", "--debug", nargs = "+", choices = ["print-md5", "ignore-ssl"], required = False, help = "Optional debug parameters")
    # subparser for mode "initialize_index"
    parser_initialize_index = subparsers.add_parser("initialize_index")
    parser_initialize_index.add_argument("archive_dir", help = "Path to local mirror of S3 bucket")
    parser_initialize_index.add_argument("project_dir", nargs = "?", default = os.getcwd(), help = "Path to the project directory (defaults to the working directory)")
    parser_initialize_index.add_argument("-d", "--debug", nargs = "+", choices = [], required = False, help = "Optional debug parameters (none currently available)")
    parser_initialize_index.add_argument("-o", "--out-path", nargs = None, required = False, help = "Output file name and path (if blank, default file name and path is used)")
    # parse args
    args = parser.parse_args()
    # return parsed args
    return args

# define Archivist class
class Archivist:
    def __init__(self):
        # parse arguments
        args = arg_parser()
        # set attributes
        # set options
        if args.mode == "prod" or args.mode == "test":
            self.options = {
                "mode": args.mode,
                "project_dir": args.project_dir,
                "uuid": args.uuid,
                "uuid_exclude": args.uuid_exclude,
                "allow_inactive": args.allow_inactive,
                "random_order": args.random_order
            }
        elif args.mode == "initialize_index":
            self.options = {
                "mode": args.mode,
                "archive_dir": args.archive_dir,
                "project_dir": args.project_dir,
                "out_path": args.out_path,
                "allow_inactive": True # option for self.load_ds()
            }
        # set log options and initialize log (for prod and test modes)
        if args.mode == "prod" or args.mode == "test":
            self.log_options = {
                "email": True if args.email else False,
                "notify": True if args.notify else False,
                "upload_log": True if args.upload_log else False
            }
            # initilize log
            self.log = {
                "log": "",
                "success": 0,
                "failure": 0,
                "failure_uuid": []
            }
        # set debug options to empty list if not given
        if args.debug is None:
            args.debug = []
        # set debug options
        self.debug = args.debug # save copy of debug for generate_rerun_code()
        self.debug_options = {
            "print_md5": True if "print-md5" in self.debug else False,
            "ignore_ssl": True if "ignore-ssl" in self.debug else False
        }
        # load config
        with open(os.path.join(self.options["project_dir"], "config.toml")) as config_file:
            self.config = toml.load(config_file)
        # load datasets.json
        with open(os.path.join(self.options["project_dir"], "datasets.json")) as json_file:
            self.ds_raw = json.load(json_file)
        # process datasets.json (for prod, test, initialize_index modes)
        if self.options["mode"] == "prod" or self.options["mode"] == "test" or self.options["mode"] == "initialize_index":
            self.ds = self.load_ds()
        # set S3 options:
        self.s3 = {
            "aws_id": os.environ["AWS_ID"],
            "aws_key": os.environ["AWS_KEY"],
            "bucket_name": os.environ["S3_BUCKET"],
            "bucket_root": os.environ["S3_ROOT"],
            "bucket_url": os.environ["S3_URL"]
        }
        # connect to S3 bucket (for prod mode)
        if self.options["mode"] == "prod":
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
        # load datasets and convert to single dictionary
        ds = {}
        if (self.options["allow_inactive"]):
            # active and inactive datasets
            datasets = self.ds_raw
            for a in datasets:
                for d in datasets[a].keys():
                    for i in range(len(datasets[a][d])):
                        ds[datasets[a][d][i]['uuid']] = datasets[a][d][i]
        else:
            # active datasets only
            datasets = self.ds_raw["active"]
            for d in datasets:
                for i in range(len(datasets[d])):
                    ds[datasets[d][i]['uuid']] = datasets[d][i]
        if self.options["mode"] == "initialize_index":
            # if mode == initialize_index, return ds
            return ds
        else:
            # else, subset datasets to be downloaded base don --uuid and --uuid-exclude
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
                print("No datasets specified. Downloading all datasets...")
            # verify dataset list is not empty
            if len(ds) == 0:
                sys.exit("No valid UUIDs specified. Exiting.")
            # shuffle order of datasets, if specified
            if self.options["random_order"]:
                ds = {k: ds[k] for k in random.sample([*ds.keys()], len(ds))}
            # return dataset list
            return ds
    
    def download_index(self):
        print("Beginning download of index...")
        d_path = os.path.join(self.options["project_dir"], "index.db")
        d_key = os.path.join(self.s3["bucket_root"], "index.db")
        self.s3["bucket"].download_file(Filename=d_path, Key=d_key)
        self.index = sqlite_utils.Database(d_path)
        print("Successfully downloaded index.")
    
    def upload_index(self):
        print("Beginning upload of index...")
        d_path = os.path.join(self.options["project_dir"], "index.db")
        d_key = os.path.join(self.s3["bucket_root"], "index.db")
        def upload_fun():
            self.s3["bucket"].upload_file(Filename=d_path, Key=d_key)
            print("Successfully uploaded index.")
            # delete local copy of index after successful upload
            os.remove(d_path)
        ## try to upload index up to 3 times
        try:
            upload_fun()
        except Exception as e:
            print(e)
            print("Failed to upload index. Retrying in 1 minute...")
            time.sleep(60)
            try:
                upload_fun()
            except Exception as e:
                print(e)
                print("Failed to upload index. Retrying in 5 minutes...")
                time.sleep(300)
                upload_fun() # don't catch exception

    def print_success_failure(self):
        total_files = str(self.log["success"] + self.log["failure"])
        print(background('Successful downloads: ' + str(self.log["success"]) + '/' + total_files, Colors.blue))
        print(background('Failed downloads: ' + str(self.log["failure"]) + '/' + total_files, Colors.red))
    
    def print_failed_uuids(self):
        for i in self.log["failure_uuid"]:
            print(i + ": " + self.ds[i]["id_name"])

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
        if self.options["allow_inactive"]:
            code += " --allow-inactive"
        if self.options["random_order"]:
            code += " --random-order"
        if len(self.debug) > 0:
            code += " --debug " + " ".join(self.debug)
        # add failed UUIDs
        if len(self.log["failure_uuid"]) > 0:
            code += " --uuid " + " ".join(self.log["failure_uuid"])
        else:
            print("No failed UUIDs found. No rerun code will be generated.")
            return None
        # return rerun code
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
    
    def initialize_index(self, archive_dir, out_path):

        """Initialize SQLite database index from local mirror of S3 bucket (e.g., created using `aws s3 sync`).

        Parameters:
            archive_path (str): Path to local mirror of S3 bucket.
            out_path (str): Path to output SQLite database index. By default, a file named 'index.db' is create in the 'dir_archive' directory.
        """

        # get output path
        if out_path is None:
            out_path = os.path.join(archive_dir, "index.db")
        
        # print output path
        print("Index will be written to: " + out_path)

        # create database
        db = sqlite_utils.Database(out_path)

        # get dataset list
        ds = self.ds
        
        # create list to hold results
        dfs = []

        # function to index a single UUID
        def index_uuid(uuid):
            # get path
            path_uuid = os.path.join(ds[uuid]['dir_parent'], ds[uuid]['dir_file'])
            path_dir = os.path.join(archive_dir, path_uuid)
            # skip if path does not exist
            if not os.path.exists(path_dir):
                print("Skipping " + uuid + " because path does not exist: " + path_dir)
                return None
            # report UUID
            print(uuid + ": " + path_dir)
            # get list of files, excluding subdirectories, and sort
            files = [f for f in os.listdir(path_dir) if os.path.isfile(os.path.join(path_dir, f))]
            files.sort()
            # create lists for file attributes
            file_name = []
            file_timestamp = []
            file_date = []
            file_size = []
            file_md5 = []
            # loop through files
            for f in files:
                # get file path
                f_path = os.path.join(path_dir, f)
                # add file name
                file_name.append(f)
                # extract timestamp from file name
                file_timestamp.append(re.search('(?<=_)(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}).*$', f).group(1))
                # extract date from file name
                file_date.append(re.search('(?<=_)(\d{4}-\d{2}-\d{2}).*$', f).group(1))
                # get file size
                file_size.append(os.path.getsize(f_path))
                # calculate file MD5 hash
                with open(f_path, 'rb') as f_data:
                    data = f_data.read()
                    f_md5 = hashlib.md5(data).hexdigest()
                file_md5.append(f_md5)
            # create dataframe
            df = pd.DataFrame({'uuid': uuid, 'file_name': file_name, 'file_timestamp': file_timestamp, 'file_date': file_date, 'file_size': file_size, 'file_md5': file_md5})
            # return dataframe
            return df
        # loop through UUIDs
        for uuid in ds.keys():
            df = index_uuid(uuid)
            dfs.append(df)
        # concatenate dataframes
        df = pd.concat(dfs, ignore_index=True)
        # convert timestamp string to epoch
        tz = self.config["project"]["tz"]
        df['file_timestamp'] = pd.to_datetime(df['file_timestamp'], format='%Y-%m-%d_%H-%M').dt.tz_localize(tz=tz).astype(int) / 10**9
        # sort by UUID and timestamp
        df = df.sort_values(by=['uuid', 'file_timestamp']).reset_index(drop=True)
        # create table of unique UUIDs and file hashes
        df_unique = df.drop_duplicates(subset=['uuid', 'file_md5', 'file_size'], keep='first')[['uuid', 'file_name', 'file_md5', 'file_size']].reset_index(drop=True)
        # mark unique files
        df_unique['file_duplicate'] = 0
        # left join to origin dataframe
        df = df.merge(df_unique[['uuid', 'file_name', 'file_duplicate']], how='left', on=['uuid', 'file_name'])
        # fill NaNs with 1
        df['file_duplicate'] = df['file_duplicate'].fillna(1)
        # create main table and insert data
        db["archive"].create({"uuid": str, "file_name": str, "file_timestamp": int, "file_date": str, "file_duplicate": int, "file_md5": str, "file_size": int})
        db["archive"].insert_all(df.to_dict("records"), batch_size=10000)

# create Archivist object
Archivist = Archivist()
