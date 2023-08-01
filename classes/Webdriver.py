# import modules
import os
import time
from selenium import webdriver # requires ChromeDriver and Chromium/Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# import classes
from archivist.classes.Archivist import Archivist as a

# define Webdriver class
class Webdriver:
    def __init__(self, tmpdir, uuid, url, wait):
        # load webdriver
        options = Options()
        options.binary_location = os.environ['CHROME_BIN']
        options.add_argument("--headless")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        prefs = {'download.default_directory' : tmpdir.name}
        options.add_experimental_option('prefs', prefs)
        chromedriver_service = Service(os.environ['CHROMEDRIVER_BIN'])
        self.wd = webdriver.Chrome(service=chromedriver_service, options=options)
        # load page
        self.wd.get(url)
        # run special processing code, if required
        self.special_processing(uuid, wait)
        # wait for page to load
        time.sleep(wait) # complete page load

    def page_source(self):
        return self.wd.page_source
    
    def special_processing(self, uuid, wait):
        proc_webdriver_path = os.path.join(a.options["project_dir"], "proc", "webdriver", uuid + ".py")
        if os.path.exists(proc_webdriver_path):
            try:
                # run code in current namespace
                proc_webdriver_code = open(proc_webdriver_path)
                exec(proc_webdriver_code.read())
                proc_webdriver_code.close()
            # print error message
            except Exception as e:
                print(e)
                raise Exception("Error in special processing code for webdriver: " + uuid)
    
    def click_css(self, wait, css):
        element = WebDriverWait(self.wd, timeout=wait).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, css)))
        element.click()

    def click_xpath(self, wait, xpath):
        element = WebDriverWait(self.wd, timeout=wait).until(
            EC.element_to_be_clickable((By.XPATH, xpath)))
        element.click()

    def click_linktext(self, wait, text):
        element = WebDriverWait(self.wd, timeout=wait).until(
            EC.element_to_be_clickable((By.LINK_TEXT, text)))
        element.click()
    
    def quit(self):
        self.wd.quit()
    
    # pass calls to unknown methods to self.wd
    # note this will fail for attributes like page_source
    # e.g., use driver.wd.page_source in this case
    def __getattr__(self, name):
        def method(*args, **kwargs):
            if (len(args) > 0 or len(kwargs) > 0):
                print("HEY")
                getattr(self.wd, name)(*args, **kwargs)
            else:
                print("HELLO")
                getattr(self.wd, name)
        return method