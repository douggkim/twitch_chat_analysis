import requests
from requests.auth import HTTPBasicAuth
import datetime

def run_airflow_dag(dag_name:str, airflow_endpoint:str, conf:dict)->None:
    """
    Args
        dag_name(string):the name of the dag to execute 
        airflow_endpoint(string): the url of the dag you will execute
    
    Returns
        None 
    """
    # Airflow API endpoint URL
    api_url = f"http://{airflow_endpoint}/api/v1/dags/{dag_name}/dagRuns"

    # Authentication credentials
    username = "airflow"
    password = "airflow"

    # Set the execution date 
    execution_date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%m:%SZ")

    # Set headers with basic authentication
    headers = {
        "Content-Type": "application/json"
    }

    # Optional payload parameters
    payload = {
        "execution_date": execution_date,
        "conf": conf
    }

    # Make the POST request
    response = requests.post(api_url, headers=headers, auth=(username, password), json=payload)

    # Check the response status code
    if response.status_code == 200:
        print("DAG run triggered successfully!")
    else:
        print("Failed to trigger DAG run:", response.text)
