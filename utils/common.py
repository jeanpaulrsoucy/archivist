# import modules
from datetime import datetime
import pytz

# import classes
from archivist.classes.Archivist import Archivist as a

# define functions
def get_datetime(ignore_fake_datetime = False):
    tz = a.config["project"]["tz"]
    if a.options["fake_datetime"] and not ignore_fake_datetime:
        t = a.options["fake_datetime"]
    else:
        t = datetime.now(pytz.timezone(tz))
    return t