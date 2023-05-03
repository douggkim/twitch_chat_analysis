import pytz
from datetime import datetime



def get_date(): 
    """get the current date + time (until miliseconds) in PDT"""
    # set the timezone to Pacific Time (US & Canada)
    timezone = pytz.timezone('America/Los_Angeles')

    # get the current time with the timezone
    current_time = datetime.now(timezone)

    # format the time as a string
    time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
    return time_str 

def get_four_digit_date():
    """get the four digit dates (ex. 2023/04/06 -> 0406) for saving the stream chats into the database"""
    today = datetime.now()
    yy_mm = today.strftime('%m%d')

    return yy_mm

