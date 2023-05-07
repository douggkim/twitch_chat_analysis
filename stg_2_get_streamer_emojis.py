import requests
import pandas as pd 
import json 
import save_to_snowflake
import os 

def get_emoji_set(channel_name:str) -> None: 
    """
    Gets the set of emoji provided to the streamer's subscribers 
    
    Args: 
    channel_name: name of the streamer
    """

    # Set up the API endpoint URL and your Twitch application client ID and secret
    url = 'https://id.twitch.tv/oauth2/token'
    client_id = os.environ["TWITCH_CLIENT_ID"]
    client_secret = os.environ["TWITCH_CLIENT_SECRET"]
    SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
    SNOWFLAKE_PW = os.environ["SNOWFLAKE_PW"]
    
    
    # Execute a query to check if the streamer's data exists
    conn, cur = save_to_snowflake.set_up_conn(snowflake_schema='twitch_data',snowflake_database='stream_data_anal',snowflake_user=SNOWFLAKE_USER,snowflake_password=SNOWFLAKE_PW, 
                                              snowflake_account='MNB68659.us-west-2',snowflake_wh='compute_wh')
    query = f"SELECT COUNT(*) FROM emojis WHERE channel_name = '{channel_name}'"
    cur.execute(query)

    # Fetch the result
    result = cur.fetchone()

    # Check if the table exists
    if result[0] > 0:
        return 

    # If there's no data Close the cursor and connection and proceed
    cur.close()
    conn.close()

    # Set up the request parameters for the authentication token
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
        'scope': 'channel_subscriptions'
    }

    # Send the authentication request
    response = requests.post(url, params=params)

    # Extract the access token from the response
    token = response.json().get('access_token')

    # Set up the API endpoint URL and your Twitch API access token
    url = 'https://api.twitch.tv/helix/users'

    # Set up the request headers
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {token}'
    }

    # Set up the query parameters for the user lookup
    params = {
        'login': channel_name
    }

    # Send the user lookup request
    response = requests.get(url, headers=headers, params=params)


    # Extract the user ID from the response
    channel_id = response.json().get('data')[0]["id"]

    # Set up the API endpoint URL and your Twitch API access token
    url = 'https://api.twitch.tv/helix/chat/emotes'

    # Set up the query parameters for the subscription lookup
    params = {
        'broadcaster_id': channel_id

    }

    # Send the subscription lookup request
    response = requests.get(url, headers=headers, params=params)

    json_str = response.content.decode('utf-8')
    json_data = json.loads(json_str)
    emoji_df = pd.DataFrame(json_data['data']).drop(["images","theme_mode","scale","format"], axis=1)
    emoji_df["channel_name"] = channel_name

    # Save the data to database
    dbengine = save_to_snowflake.set_up_engine(snowflake_schema='twitch_data',snowflake_database='stream_data_anal',snowflake_user=SNOWFLAKE_USER ,snowflake_password=SNOWFLAKE_PW, snowflake_account='MNB68659.us-west-2',snowflake_wh='compute_wh', snowflake_role="accountadmin")
    emoji_df.to_sql(name="emojis",con=dbengine, if_exists="replace", index=False)


get_emoji_set(channel_name="amouranth")