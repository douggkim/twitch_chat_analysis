import pandas as pd 
import os 
from datetime import datetime, timedelta
import get_date
import save_to_snowflake

def get_emoji_stats(channel_name:str, query_date:datetime=datetime.now(), yy_mm:str=get_date.get_four_digit_date()) -> str: 
    """
    gets the list of streamer customer emojis and see how they're used in the chats 

    Args: 
        channel_name(string) : the streamer name or the channel_name 
        query_date (datetime) : get the chats until that datetime 
        yy_mm (sting) : four digit date (ex. 04/23 -> 0423) when the stream started 
    
    return: 
        returns the name of the channel to be used in the next airflow task 
    """
    from_date = query_date.strftime("%Y-%m-%d %H:%M")
    SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
    SNOWFLAKE_PW = os.environ["SNOWFLAKE_PW"]
    
    # Establish snowflake connection 
    conn, cur = save_to_snowflake.set_up_conn(snowflake_schema='twitch_data',snowflake_database='stream_data_anal',snowflake_user=SNOWFLAKE_USER,snowflake_password=SNOWFLAKE_PW, 
                                              snowflake_account='MNB68659.us-west-2',snowflake_wh='compute_wh')

    emoji_query = f"SELECT NAME FROM emojis WHERE channel_name = '{channel_name}'"
    chat_query = f"SELECT * FROM twitch_chats WHERE channel_name = '{channel_name}' AND channel_date = '{yy_mm}' AND MESSAGE_DATE <= '{from_date}'"

    # Fetch the results 
    # fetch the set of emojis 
    emoji_df = pd.read_sql_query(emoji_query, conn)
    # Fetch the recent chats 
    chat_df = pd.read_sql_query(chat_query, conn)
    
    # extract the names of the emojis
    emoji_names = emoji_df['NAME'].tolist()

    # create a dictionary to store the occurrences of each emoji
    emoji_stats_df = pd.DataFrame(emoji_names, columns=["emoji_name"])
    emoji_stats_df ["count"] = 0

    # iterate through each chat
    for chat in chat_df['MESSAGE_TEXT']:
        # iterate through each emoji name and count the occurrences
        for i, name in enumerate(emoji_names):
            count = chat.count(name)
            emoji_stats_df.loc[i, 'count'] += count
    
    # Add additional information required for the dfs 
    emoji_stats_df["channel_date"] = yy_mm
    emoji_stats_df["channel_name"] = channel_name
    
    # Reorder columns for insert 
    emoji_stats_df = emoji_stats_df[["channel_name","channel_date","emoji_name","count"]] 
    
    # Delete if there was earlier analysis 
    delete_query = f"DELETE FROM emoji_stats WHERE channel_name = '{channel_name}' AND channel_date = '{yy_mm}'"
    cur.execute(delete_query)
    
    #save the data to db
    dbengine = save_to_snowflake.set_up_engine(snowflake_schema='twitch_data',snowflake_database='stream_data_anal',snowflake_user=SNOWFLAKE_USER,snowflake_password=SNOWFLAKE_PW, snowflake_account='MNB68659.us-west-2',snowflake_wh='compute_wh', snowflake_role="accountadmin")
    emoji_stats_df.to_sql(name="emoji_stats", con=dbengine, if_exists="append", index=False)
    
    return channel_name


    
    