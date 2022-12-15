# import modules
import os
from datetime import datetime
import time
import tempfile
from zipfile import ZipFile
import hashlib
from humanfriendly import parse_size, format_size
from colorit import *
import requests
import urllib3
import ssl
import pandas as pd

# import classes
from archivist.classes.Archivist import Archivist as a
from archivist.classes.Webdriver import Webdriver

# import functions
from archivist.utils.common import get_datetime

# define Downloader class
class Downloader:
    def __init__(self, uuid):
        # set retry count
        self.retry = -1 # initial try sets count to 0
        # get UUID info
        self.uuid_info = self.get_dataset_info(uuid)
        # wait before beginning download (0 seconds by default)
        time.sleep(a.config["downloading"]["wait_before_downloads"])
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
            if k == "min_size":
                return parse_size(v)
            else:
                return(int(v))
        except:
            print("Error interpreting arg " + k + ", setting value to 0.")
            return 0
    
    def get_dataset_info(self, uuid):
        d = a.ds[uuid]
        uuid_info = {"uuid": uuid}
        # verify dataset is active
        if a.options["allow_inactive"] is not True and d["active"] != "True":
            raise Exception(uuid + ": Dataset is marked as inactive, skipping...")
        # report ID name
        print(d["id_name"])
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
                # write error to URL (failure will be handled by the dl_fun)
                uuid_info["url"] = "ERROR"
        else:
            raise Exception(uuid + ": Neither a URL nor a URL function are given, skipping...")
        # get file name, path and extension
        uuid_info["file_name"] = d["file_name"]
        uuid_info["file_path"] = os.path.join(d["dir_parent"], d["dir_file"], uuid_info["file_name"])
        uuid_info["file_ext"] = "." + d["file_ext"]
        # process other arguments
        uuid_info["args"] = {}
        # process bool args
        bool_args = [
            "user", "rand_url", "verify",
            "legacy_ssl", "unzip", "js"
            ]
        for k, v in d["args"].items():
            if k in bool_args:
                uuid_info["args"][k] = self.arg_bool(k, v)
        # process int args
        int_args = [
            "wait", "min_size", "width", "height"
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
    def index_entry(self, uuid, f_name, f_timestamp, f_path):
        # get file size
        f_size = os.path.getsize(f_path)
        # get file md5
        f_md5 = hashlib.md5(open(f_path, 'rb').read()).hexdigest()
        # extract date and convert timestamp
        tz = a.config["project"]["tz"]
        f_timestamp = pd.to_datetime(f_timestamp, format='%Y-%m-%d_%H-%M').tz_localize(tz=tz)
        f_date = str(f_timestamp.date())
        f_timestamp = f_timestamp.value / 10**9
        # check if file is a duplicate using db
        db = a.index
        query = db.execute("SELECT COUNT(*) FROM archive WHERE uuid = ? AND file_md5 = ? AND file_size = ?", (uuid, f_md5, f_size))
        f_duplicate = 1 if query.fetchone()[0] > 0 else 0
        # create index entry
        f_index = {
            "uuid": uuid,
            "file_name": f_name,
            "file_timestamp": f_timestamp,
            "file_date": f_date,
            "file_duplicate": f_duplicate,
            "file_size": f_size,
            "file_md5": f_md5
            }
        # return index entry
        return f_index
    
    def insert_index(self, f_index):
        # insert index entry into database
        a.index["archive"].insert(f_index)
    
    def upload_file(self, f_name, f_path, uuid, f_index):
        # generate full S3 key
        f_key = os.path.join(a.s3["bucket_root"], f_name)
        # upload file to S3
        try:
            # upload file
            if f_index["file_duplicate"] == 0:
                a.s3["bucket"].upload_file(Filename=f_path, Key=f_key)
            else:
                print("File is a duplicate. Skipping upload...")
            # insert index entry and record success
            self.insert_index(f_index)
            a.record_success(f_name)
        except Exception as e:
            # print error message
            print(e)
            # record failure
            a.record_failure(f_name, uuid)
    
    def dl_fun(self, uuid_info):
        # get download function
        dl_fun = uuid_info["dl_fun"]
        # set uuid
        uuid = uuid_info["uuid"]
        # set file name with timestamp and file ext
        f_timestamp = get_datetime().strftime('%Y-%m-%d_%H-%M')
        f_name = uuid_info["file_path"] + '_' + f_timestamp + uuid_info["file_ext"]
        # begin download
        while self.retry < a.config["downloading"]["max_retries"]:
            try:
                # announce retry
                if self.retry >= 0:
                    print(background("Retry " + str(self.retry + 1) + "/" + str(a.config["downloading"]["max_retries"]) + " for " + uuid, Colors.orange))
                # download file
                getattr(self, dl_fun)(uuid_info, f_name, f_timestamp)
                break # function ran without exceptions
            except Exception as e:
                # print error message
                print(e)
                # increment retry counter
                self.retry += 1
                # record failure if maximum retries reached
                if self.retry == a.config["downloading"]["max_retries"]:
                    # record failure
                    a.record_failure(f_name, uuid)

    def dl_file(self, uuid_info, f_name, f_timestamp):
        # set UUID and URL
        uuid = uuid_info["uuid"]
        url = uuid_info["url"]

        # set default parameters
        html = True if uuid_info["file_ext"] == ".html" else False
        verify = uuid_info["args"]["verify"] if "verify" in uuid_info["args"] else True
        legacy_ssl = uuid_info["args"]["legacy_ssl"] if "legacy_ssl" in uuid_info["args"] else False
        user = uuid_info["args"]["user"] if "user" in uuid_info["args"] else False
        rand_url = uuid_info["args"]["rand_url"] if "rand_url" in uuid_info["args"] else False
        unzip = uuid_info["args"]["unzip"] if "unzip" in uuid_info["args"] else False
        min_size = uuid_info["args"]["min_size"] if "min_size" in uuid_info["args"] else False

        # DEBUG: override 'verify' parameter for requests
        if a.debug_options["ignore_ssl"]:
            verify = False

        # temporary file name
        tmpdir = tempfile.TemporaryDirectory()
        f_path = os.path.join(tmpdir.name, uuid_info["file_name"] + uuid_info["file_ext"])

        # download file
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
        if legacy_ssl:
            # workaround for unsafe_legacy_renegotiation error
            # https://github.com/scrapy/scrapy/issues/5491#issuecomment-1241862323
            class CustomHttpAdapter (requests.adapters.HTTPAdapter):
                def __init__(self, ssl_context=None, **kwargs):
                    self.ssl_context = ssl_context
                    super().__init__(**kwargs)
                def init_poolmanager(self, connections, maxsize, block=False):
                    self.poolmanager = urllib3.poolmanager.PoolManager(
                        num_pools=connections, maxsize=maxsize,
                        block=block, ssl_context=self.ssl_context)
            def get_legacy_session():
                ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
                ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
                session = requests.session()
                session.mount('https://', CustomHttpAdapter(ctx))
                return session
            # make request
            if (verify is False or a.debug_options["ignore_ssl"]):
                # if verify is False, ge the following error: Cannot set verify_mode to CERT_NONE when check_hostname is enabled.
                print("WARNING: Ignoring verify: False for legacy SSL request.")
            req = get_legacy_session().get(url, headers=headers, verify=True)
        else:
            req = requests.get(url, headers=headers, verify=verify)

        ## check if request was successful
        if not req.ok:
            # raise exception
            raise Exception("Request failed")
        # check if page source is above minimum expected size
        if html and min_size:
            if len(req.text.encode("utf-8")) < min_size:
                # raise exception
                raise Exception("Page source is below minimum expected size (" + format_size(min_size) + ")")
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
            # unzip file, if required
            if unzip:
                # unzip data
                z_path = os.path.join(tmpdir.name, "zip_file.zip")
                with open(z_path, mode="wb") as local_file:
                    local_file.write(req.content)
                with ZipFile(z_path, "r") as zip_file:
                    zip_file.extractall(tmpdir.name)
            else:
                # all other data: write contents to temporary file
                with open(f_path, mode="wb") as local_file:
                    local_file.write(req.content)
            # prepare index entry
            f_index = self.index_entry(uuid, f_name, f_timestamp, f_path)
            # upload file if file is not a duplicate then insert index entry
            self.upload_file(f_name, f_path, uuid, f_index)

    def html_page(self, uuid_info, f_name, f_timestamp):

        # set UUID and URL
        uuid = uuid_info["uuid"]
        url = uuid_info["url"]
        
        # temporary file name
        tmpdir = tempfile.TemporaryDirectory()
        f_path = os.path.join(tmpdir.name, uuid_info["file_name"] + uuid_info["file_ext"])

        # set default parameters
        # user = uuid_info["args"]["user"] if "user" in uuid_info["args"] else False
        wait = uuid_info["args"]["wait"] if "wait" in uuid_info["args"] else 0
        min_size = uuid_info["args"]["min_size"] if "min_size" in uuid_info["args"] else False

        # download file
        # load page and get source
        driver = Webdriver(tmpdir, uuid, url, wait)
        page_source = driver.page_source()
        # check if page source is above minimum expected size
        if min_size:
            if len(page_source.encode("utf-8")) < min_size:
                # raise exception
                raise Exception("Page source is below minimum expected size (" + format_size(min_size) + ")")
        # DEBUG: print md5 hash of dataset
        if a.debug_options["print_md5"]:
            self.print_md5(page_source.encode("utf-8"))
        # save HTML file
        with open(f_path, "w") as local_file:
            local_file.write(page_source)
        # verify download
        if not os.path.isfile(f_path):
            # raise exception
            raise Exception("File not found")
        # successful request: if mode == test, print success and end
        elif a.options["mode"] == "test":
            # record success
            a.record_success(f_name)
        # successful request: mode == prod, prepare files for data upload
        else:
            # prepare index entry
            f_index = self.index_entry(uuid, f_name, f_timestamp, f_path)
            # upload file if file is not a duplicate then insert index entry
            self.upload_file(f_name, f_path, uuid, f_index)
        # quit webdriver
        driver.quit()
