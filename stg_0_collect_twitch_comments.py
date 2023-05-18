import requests 
import socket
import re
import get_date
import save_to_snowflake
import json, os 
import threading
import requests
from airflow.models import DAG, DagRun
from airflow.utils.types import DagRunType
import datetime

# Set up authorization parameters
CLIENT_ID = os.environ["TWITCH_CLIENT_ID"]
CLIENT_SECRET = os.environ["TWITCH_CLIENT_SECRET"]
SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PW = os.environ["SNOWFLAKE_PW"]

REDIRECT_URI = "http://localhost"
AUTH_URL = "https://id.twitch.tv/oauth2/authorize"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"

# Generate authorization URL for user to visit
params = {
    "client_id": CLIENT_ID,
    "redirect_uri": REDIRECT_URI,
    "response_type": "code",
    "scope": "chat:read",
}
auth_url = f"{AUTH_URL}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
print(f"Visit this URL to authorize your Twitch account: {auth_url}")

# Wait for user to authorize and receive authorization code
oauth_token = input("Enter the authorization code from the redirect URL: ")
conn, cur = save_to_snowflake.set_up_conn(snowflake_schema='twitch_data',snowflake_database='stream_data_anal',snowflake_user=SNOWFLAKE_USER,snowflake_password=SNOWFLAKE_PW, 
                                          snowflake_account='MNB68659.us-west-2',snowflake_wh='compute_wh')
# Set the parameters for Twitch
HOST = "irc.chat.twitch.tv"
PORT = 6667
NICK = "dougie_duk"
PASS = f'{oauth_token}'
CHANNEL = input("enter the name of the streamer you want to view:")

# Connect to Twitch IRC server
s = socket.socket()
s.connect((HOST, PORT))

# get the four digit date for updating into channel_date column 
yy_mm = get_date.get_four_digit_date()

# Send authentication credentials to server
s.send(f"PASS {PASS}\r\n".encode("utf-8"))
s.send(f"NICK {NICK}\r\n".encode("utf-8"))
s.send(f"JOIN #{CHANNEL}\r\n".encode("utf-8"))

# Define the dag we will run 
dag_id = "twitch_real_time_analysis"
# Define the DAG object
dag = DAG(dag_id=dag_id, start_date=datetime.now())
# Trigger the DAG using DagRun and pass the channel name as the parameter
DagRun.schedule_dag(dag_id=dag_id, execution_date=datetime.now(), run_type=DagRunType.MANUAL, conf={'channel_name': CHANNEL})




# Start receiving and processing chat messages
# Use regex to parse messages
while True:
    response = s.recv(2048).decode("utf-8")
    
    if response.startswith("PING"):
        s.send("PONG\n".encode("utf-8"))
    else:
        message = re.search(f"PRIVMSG #(.+) :(.+)", response)
        writer = re.search(f"(\w+)!", response)

        if message and writer:
            print(f"{get_date.get_date()} {writer.group(1)}: {message.group(2)}")
            # t = threading.Thread(target=insert_to_snowflake, args=(channel_name, message_text))
            try: 
                t = threading.Thread(target=save_to_snowflake.insert_to_snowflake, args=(CHANNEL, yy_mm, writer.group(1), message.group(2), conn, cur, 'twitch_chats'))
            except Exception as e: 
                print("error!")
                print(f"the data : {get_date.get_date()} {writer.group(1)}: {message.group(2)}")
                print(e)


            t.start()
