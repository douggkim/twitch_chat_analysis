import snowflake.connector
import pandas as pd
from sqlalchemy import create_engine 


def set_up_engine(snowflake_account, snowflake_user, snowflake_password, snowflake_database, snowflake_schema, snowflake_wh, snowflake_role):
    """Inserts the message info to snowflake. 
    channel_name : name of the channel 
    message_author: author of the chat 
    message_text: text content
    snowflake_conn: connection to snowflake
    snowflake_cursor: snowflake table cursor 
    table: table to insert the data in  """
    # connection_string = f'snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}.snowflakecomputing.com/{snowflake_database}/{snowflake_schema}?warehouse={snowflake_wh}&role={snowflake_role}'
    connection_string = f'snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{snowflake_database}/{snowflake_schema}?warehouse={snowflake_wh}&role={snowflake_role}'
    # Create the dbengine object
    dbengine = create_engine(connection_string)
    return dbengine


def set_up_conn(snowflake_account, snowflake_user, snowflake_password, snowflake_database, snowflake_schema, snowflake_wh): 
    # Set up Snowflake connection parameters
    SNOWFLAKE_ACCOUNT = snowflake_account
    SNOWFLAKE_USER = snowflake_user
    SNOWFLAKE_PASSWORD = snowflake_password
    SNOWFLAKE_DATABASE = snowflake_database
    SNOWFLAKE_SCHEMA = snowflake_schema
    SNOWFLAKE_WAREHOUSE = snowflake_wh


    # Set up Snowflake connection and cursor
    snowflake_conn = snowflake.connector.connect(account=SNOWFLAKE_ACCOUNT, user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA, warehouse=SNOWFLAKE_WAREHOUSE)
    snowflake_cursor = snowflake_conn.cursor()

    return snowflake_conn, snowflake_cursor 

# Define function to handle Snowflake insertion in separate thread
def insert_to_snowflake(channel_name, channel_date, message_author, message_text, snowflake_conn, snowflake_cursor, table_name):
    """Inserts the message info to snowflake. 
    channel_name : name of the channel 
    message_author: author of the chat 
    message_text: text content
    snowflake_conn: connection to snowflake
    snowflake_cursor: snowflake table cursor 
    table: table to insert the data in  """
    # Define insert statement
    # insert_stmt = f"INSERT INTO {table_name} (channel_name, channel_date, message_author, message_text) VALUES ('{channel_name}', '{channel_date}', '{message_author}', '{message_text}')"
    insert_stmt = f"INSERT INTO {table_name} (channel_name, channel_date, message_author, message_text) VALUES (%s, %s, %s, %s)"
    values = (channel_name, channel_date, message_author, message_text)
    # Execute insert statement
    # snowflake_conn.cursor().execute(insert_stmt)
    snowflake_cursor.execute(insert_stmt, values)
    
    # Commit changes
    snowflake_conn.commit()

    return snowflake_cursor

def upsert_to_snowflake(data, snowflake_connection, table_name, upsert_col):
    """
    Upsert data into an existing Snowflake table using the 'id' column.

    Args:
        data (pd.DataFrame): Data to be upserted
        snowflake_connection (snowflake.connector.connect): Snowflake connection object
        table_name (str): Name of the table to upsert data to
        upsert_col (str): Name of the column you will use to upsert
    """
    # Create a cursor object
    cursor = snowflake_connection.cursor()

    # Create temporary table to hold the data
    temp_table_name = 'temp_' + table_name
    create_temp_table_sql = f"CREATE TEMPORARY TABLE {temp_table_name} (LIKE {table_name})"
    cursor.execute(create_temp_table_sql)

    # Write the data to the temporary table
    data.to_sql(temp_table_name, snowflake_connection, if_exists='replace', index=False)

    # Upsert the data from the temporary table to the target table
    upsert_sql = f"""
        MERGE INTO {table_name} AS tgt
        USING {temp_table_name} AS src
        ON tgt.id = src.id
        WHEN MATCHED THEN
            UPDATE SET {', '.join([f"{col}=src.{col}" for col in data.columns if col != {upsert_col}])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(data.columns)})
            VALUES ({', '.join([f"src.{col}" for col in data.columns])})
    """
    cursor.execute(upsert_sql)

    # Commit the transaction and close the cursor
    cursor.execute('COMMIT')
    cursor.close()


# Define function to update column based on list of IDs
def update_column_with_ids(ids_list, change_column_name, change_column_value, target_column_name, table_name, snowflake_connection):
    cursor = snowflake_connection.cursor()

    # Build the SQL update statement
    sql = f"UPDATE {table_name} SET {change_column_name} = '{change_column_value}' WHERE {target_column_name}  IN ({','.join(ids_list)})"

    cursor.execute(sql)

    cursor.close()
