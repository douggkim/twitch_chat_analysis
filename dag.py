from datetime import timedelta, datetime 
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago 
from stg_1_get_channel_name import get_channel_name
from stg_2_get_streamer_emojis import get_emoji_set
from stg_3_sentiment_analysis import sentiment_analysis 
from stg_3_1_process_subs import process_sub_info
from stg_3_2_summary_sentence import summarize_comment
from stg_3_4_word_cloud_generation import count_words
from airflow.models import Variable
import get_date


default_args = {
    "owner": "Doug Kim",
    "depends_on_past": False,
    "email": ("slakingex@gmail.com"),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries":1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "twitch_real_time_analysis", 
    default_args = default_args,
    start_date=datetime(2023, 5, 1),
    # Every 5 minutes
    # schedule_interval='*/5 * * * *',
    # Do not run all the tasks that were not ran since the starting date 
    catchup = False, 
    description = "run analysis on twitch comments every 5 minutes",
)


dag_setup = PythonOperator(
    task_id = 'get_channel_name',
    python_callable=get_channel_name,
    dag=dag
)

get_emoji_set_task = PythonOperator(
    task_id = 'get_emoji_set',
    python_callable=get_emoji_set,
    op_kwargs={'channel_name': dag_setup.output},
    dag=dag
)

process_sub_info_task = PythonOperator(
    task_id = 'process_sub_info',
    python_callable=process_sub_info,
    op_kwargs={'channel_name': dag_setup.output, 'channel_date': get_date.get_four_digit_date()},
    dag = dag
)

sentiment_analysis_task = PythonOperator(
    task_id = 'sentiment_analysis',
    python_callable=sentiment_analysis,
    op_kwargs={'channel_name': dag_setup.output, 'channel_date': get_date.get_four_digit_date()},
    dag = dag
)

sentence_summary_task = PythonOperator(
    task_id = 'sentence_summary',
    python_callable=summarize_comment,
    op_kwargs={'channel_name': dag_setup.output, 'query_date': datetime.now() },
    dag = dag
)

word_cloud_task = PythonOperator(
    task_id = 'word_cloud_generation',
    python_callable=count_words,
    op_kwargs={'channel_name': dag_setup.output, 'query_date':datetime.now(),'yy_mm': get_date.get_four_digit_date()},
    dag = dag
)


# Set up the pipeline and run other tasks simultaneously
dag_setup>>get_emoji_set_task>>[sentiment_analysis_task,process_sub_info_task, sentence_summary_task, word_cloud_task]