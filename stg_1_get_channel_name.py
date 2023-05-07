from airflow.models import Variable
import time

def get_channel_name(**context) -> str:
    channel_name = Variable.get("channel_name")
    # Wait 5 minutes to start running the pipeline
    time.sleep(300)

    return channel_name