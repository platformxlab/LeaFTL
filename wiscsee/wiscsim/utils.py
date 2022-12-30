import datetime as dt
import sys

KB = 1024
MB = 1024**2
GB = 1024**3

DEBUG = True
def log_msg(*msg):
    '''
    Log a message with the current time stamp.
    '''
    msg = [str(_) for _ in msg]
    if DEBUG:
        print"[%s] %s" % ((dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), " ".join(msg))

    sys.stdout.flush()