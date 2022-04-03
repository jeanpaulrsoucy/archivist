# import modules
from datetime import datetime
import pytz

# define functions
def get_datetime():
    tz = 'America/Toronto'
    t = datetime.now(pytz.timezone(tz))
    return t