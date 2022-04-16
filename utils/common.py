# import modules
from datetime import datetime
import pytz

# import classes
from archivist.classes.Archivist import Archivist as a

# define functions
def get_datetime():
    tz = a.config["project"]["tz"]
    t = datetime.now(pytz.timezone(tz))
    return t