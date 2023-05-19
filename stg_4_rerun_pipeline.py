from datetime  import datetime, timedelta
import run_airflow_dag
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.api.common.experimental.trigger_dag import trigger_dag
from pytz import timezone

def rerun(**context):
    channel_name = context['task_instance'].xcom_pull(task_ids='get_channel_name',key='channel_name')
    query_date = context['task_instance'].xcom_pull(task_ids='get_channel_name',key='query_date')
    
    query_date = datetime.fromisoformat(query_date)
    query_date = timezone('America/Los_Angeles').localize(query_date) 
    new_query_date = (query_date + timedelta(minutes=5))
    # Define the dag we will run 
    dag_id = "twitch_real_time_analysis"
    conf={'channel_name': channel_name, 'query_date':new_query_date.isoformat()}


    dag = DAG(dag_id, start_date=query_date)  
    trigger_dag(dag_id, run_id=f"rerun_{new_query_date}", execution_date=query_date,conf=conf)