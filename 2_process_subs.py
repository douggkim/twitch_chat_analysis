import save_to_snowflake
import pandas as pd
import re 
import os


# define support functions
def extract_gift_info(text:str):
    # compare with the general pattern
    match1 = re.search(r'(\w+) just gifted (\d+) subs!', text)
    if match1:
        user = match1.group(1)
        num_subs = match1.group(2)
        return user, int(num_subs)
    
    # Extract username
    username_pattern = r'^(\w+)'
    username_match = re.search(username_pattern, text)
    if username_match: 
        username = username_match.group(1)

        # Extract number of subscriptions
        sub_pattern = r'sub(scription)?'
        sub_matches = re.findall(sub_pattern, text)
        num_subs = len(sub_matches)

        return username, num_subs

def extract_resub(text:str): 
    # Extract username
    username_pattern = r'^(\w+)'
    username_match = re.search(username_pattern, text)
    if username_match:
        username = username_match.group(1)

    # Extract number of subscriptions
    sub_pattern = r'Resubbing for (\d+)'
    sub_match = re.search(sub_pattern, text)
    if sub_match: 
        num_subs = int(sub_match.group(1))
        return username, num_subs
    else: 
        # Extract number of subscriptions
        sub_pattern = r'resubscribed for (\d+)'
        sub_match = re.search(sub_pattern, text)
        num_subs = int(sub_match.group(1))
        return username, num_subs

def process_sub_info(channel_name:str, channel_date:str) -> None : 
    SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
    SNOWFLAKE_PW = os.environ["SNOWFLAKE_PW"]

    conn, cur = save_to_snowflake.set_up_conn(snowflake_schema='twitch_data',snowflake_database='stream_data_anal',snowflake_user=SNOWFLAKE_USER,snowflake_password=SNOWFLAKE_PW, snowflake_account='MNB68659.us-west-2',snowflake_wh='compute_wh')

    # Get the gift data 
    gift_query = f"SELECT * FROM twitch_chats WHERE channel_name='{channel_name}' AND channel_date='{channel_date}' AND lower(message_text) LIKE '%gifted%' AND message_author like '%bot' AND sub_processed = FALSE"
    gift_data = cur.execute(gift_query).fetchall()
    gift_df = pd.DataFrame(gift_data)

    # Process the gift data 
    gift_result_df = pd.DataFrame()
    gift_result_df["chat_id"]  = gift_df[0]
    gift_result_df[["message_author","sub_month"]]=gift_df[5].apply((lambda x: pd.Series(extract_gift_info(x))))
    gift_result_df["message_date"] = gift_df[3]
    gift_result_df["channel_name"] = gift_df[1]
    gift_result_df["channel_date"] = gift_df[2]
    gift_result_df["sub_type"] = "gift"

    # Get the resub data 
    sub_query = f"SELECT * FROM twitch_chats WHERE channel_name='{channel_name}' AND channel_date='{channel_date}' AND lower(message_text) LIKE '%month%' AND message_author like '%bot' AND sub_processed = FALSE"
    resub_data = cur.execute(sub_query).fetchall()
    resub_df = pd.DataFrame(resub_data)

    # process the resub data 
    resub_result_df = pd.DataFrame()
    resub_result_df["chat_id"]  = resub_df[0]
    resub_result_df[["message_author","sub_month"]]=resub_df[5].apply((lambda x: pd.Series(extract_resub(x))))
    resub_result_df["message_date"] = resub_df[3]
    resub_result_df["channel_name"] = resub_df[1]
    resub_result_df["channel_date"] = resub_df[2]
    resub_result_df["sub_type"] = "resub"

    # Prepare the insert data 
    insert_df = pd.concat([resub_result_df, gift_result_df], axis=0)
    insert_df['message_date'] = insert_df['message_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # insert_df['message_date'] = pd.to_datetime(insert_df['message_date'])
    print(insert_df.head())

    # Save the data to database
    dbengine = save_to_snowflake.set_up_engine(snowflake_schema='twitch_data',snowflake_database='stream_data_anal',snowflake_user='kimkim',snowflake_password='Evlngood2!', snowflake_account='MNB68659.us-west-2',snowflake_wh='compute_wh', snowflake_role="accountadmin")
    insert_df.to_sql(name="subscription_info",con=dbengine, if_exists="append", index=False)
    
    # mark the data as processed
    id_list = list(insert_df['chat_id'].apply(lambda x: str(x)))
    save_to_snowflake.update_column_with_ids(ids_list=id_list, change_column_name="sub_processed",change_column_value='TRUE', target_column_name="chat_id", table_name="twitch_chats",snowflake_connection=conn)

    conn.commit() 

    cur.close() 
    conn.close() 
    
    return True 

process_sub_info(channel_name="amouranth", channel_date="0423")

