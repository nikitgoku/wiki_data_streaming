# ------------------------------------------------------------ #
#  Version    : 0.1    							               #
#  Created On : 12-Sept-2024 0:35:59                           #
#  Created By : Nikit Gokhale                                  #
#  Script     : Python                                         #
#  Notes      : Script to create airflow DAG for Kafka         #
#               streaming                                      #
# ------------------------------------------------------------ #

from airflow import DAG
from airflow.operators.python import PythonOperator

from kafka import KafkaProducer

from datetime import date
from datetime import datetime
from datetime import timedelta

import requests
import os
import json
import time
import logging

# Get the start date
start_date = datetime(2024, 8, 27)


def get_wiki_data():
    """
    This function utilizes the WikiMedia's RecentChanges API
    to request a list of all the recent changes to the wiki
    """
    URL = "https://en.wikipedia.org/w/api.php"
    PARAMS = {
        "format": "json",
        "rcprop": "title|ids|sizes|flags|user|timestamp|loginfo",
        "list": "recentchanges",
        "action": "query",
        "rclimit": "1"
    }
    # Get the response from the URL
    response = requests.get(url=URL, params=PARAMS)
    data = response.json()

    # print(data['query']['recentchanges'])
    return data


def format_wiki_data(data):
    """
    This function formats the wiki recent changes data and takes only the required data from API
    """
    wiki_data = {'title': data['query']['recentchanges'][0]['title'],
                 'time': data['query']['recentchanges'][0]['timestamp'],
                 'username': data['query']['recentchanges'][0]['user'],
                 'change_type': data['query']['recentchanges'][0]['type'],
                 'pageId': data['query']['recentchanges'][0]['pageid'],
                 'oldsize': data['query']['recentchanges'][0]['oldlen'],
                 'newsize': data['query']['recentchanges'][0]['newlen'],
                 'revisionId': data['query']['recentchanges'][0]['revid'],
                 'old_revisionId': data['query']['recentchanges'][0]['old_revid']}

    # print(wiki_data)
    return wiki_data


def stream_wiki_data():
    """
    This function streams the data into one of Kakfa topic using Kafka Producer API
    """
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    current_time = time.time()

    while True:
        if time.time() > current_time + 60:
            break
        try:
            wiki_api_data = get_wiki_data()
            formatted_wiki_data = format_wiki_data(wiki_api_data)

            producer.send('recent_changes', json.dumps(formatted_wiki_data).encode('utf-8'))
        except Exception as e:
            logging.error(f"ERROR: An error occurred: {e}")
            continue

# Default arguments for Airflow
default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

# Create a single node dag which utilises the 'stream_wiki_data' function to
# write into one of the Kafka topic using the Producer API
with DAG(
    "wiki_data_dag",
    default_args=default_args,
    description="Collecting Wikipedia data for recent changes done on any Wikipedia article",
    schedule_interval="@daily",
    catchup=False
) as dag:

    streaming = PythonOperator(
        task_id="streaming_wiki_data",
        python_callable= stream_wiki_data
    )

