# import modules
import os
from datetime import datetime
import tempfile
from zipfile import ZipFile
import hashlib
from humanfriendly import parse_size, format_size
from colorit import *
import requests

# import classes
from archivist.classes.Archivist import Archivist as a
from archivist.classes.Webdriver import Webdriver

# import functions
from archivist.utils.common import get_datetime

# define Downloader class
class Downloader:
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
            "unzip", "js"
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
        min_size = uuid_info["args"]["min_size"] if "min_size" in uuid_info["args"] else False

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
                # check if page source is above minimum expected size
                if html and min_size:
                    if len(req.text.encode("utf-8")) < min_size:
                        # record failure
                        a.record_failure(f_name, uuid)
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
                    # upload file
                    self.upload_file(f_name, f_path, uuid)
        except Exception as e:
            # print error message
            print(e)
            # record failure
            a.record_failure(f_name, uuid)

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
        # user = uuid_info["args"]["user"] if "user" in uuid_info["args"] else False
        wait = uuid_info["args"]["wait"] if "wait" in uuid_info["args"] else 0
        min_size = uuid_info["args"]["min_size"] if "min_size" in uuid_info["args"] else False

        # download file
        try:
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
                # upload file
                self.upload_file(f_name, f_path, uuid)
            # quit webdriver
            driver.quit()
        except Exception as e:
            # print error message
            print(e)
            # record failure
            a.record_failure(f_name, uuid)
