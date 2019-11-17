"""Defines a number of gcp Python functions to be deployed.

Currently includes:
1. Cloud Storage update function (runs on a CRON job to store FPL JSON data in a Blob)
2. BigQuery table creation function (triggered by addition of a blob to Cloud Storage)
"""

import requests as rq
import pandas as pd
from flask import Request

from gcloud import storage

import logging
import datetime as dt
import json
from io import BytesIO

FPL_BUCKET_NAME = 'fpl_fun'
FPL_PROJECT_ID = 'fpl-fun'


def _fpl_downloader():
    response = rq.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    return response.content


def get_fpl_data(req: Request) -> str:
    """Cloud function to save data from FPL website.

    Writes raw JSON to Google Cloud storage.

    Args:
      req: Flask Request automatically passed by gcp Functions. Is ignored.

    Returns:
      Filename that was written to Google Cloud storage

    """
    del req
    storage_client = storage.Client()

    logging.info("Beginning FPL data collection run.")

    response_content = _fpl_downloader()

    fpl_bucket = storage_client.bucket(FPL_BUCKET_NAME)

    fpl_blob_name = 'fpl_data_' + dt.datetime.utcnow().isoformat()
    new_blob = fpl_bucket.blob(fpl_blob_name)
    new_blob.upload_from_file(BytesIO(response_content), size=2048)

    logging.info(f"Data uploaded to {new_blob.name}.")

    return new_blob.name


def write_gcp_tables_pubsub(data, context) -> bool:
    """Triggered by creation of a new blob in our fpl_data storage bucket.

    Represents the full state of the FPL website at a given point in time.

    This function sequentially writes the blob data to 3 key BigQuery tables:
    1. Team information -> information about each team -- strength as well as fixture
    2. Player information -> information on each premier league player at a given point in time
    3. Events information -> information about each upcoming event
    4. Fixtures information -> information about all fixtures in an upcoming event

    :param data: Passed in by the Pub/Sub service
    :param context: Passed in by the Pub/Sub service
    :return: True if function successfully executes properly
    """
    bucket_name = data['bucket']
    blob_name = data['name']

    storage_client = storage.Client()
    fpl_bucket = storage_client.bucket(bucket_name)

    blob_data_pointer = fpl_bucket.get_blob(blob_name)
    blob_data_json = json.loads(blob_data_pointer.download_as_string())

    logging.info('Successfully loaded blob data')

    teams_df = pd.DataFrame.from_records(blob_data_json['teams'])
    players_df = pd.DataFrame.from_records(blob_data_json['elements'])
    events_df = pd.DataFrame.from_records(blob_data_json['events'])
    fixtures_df = pd.DataFrame.from_records(blob_data_json['next_event_fixtures'])

    logging.info('Successfully created dataframes')

    table_names = ['teams', 'players', 'events', 'fixtures']
    today = dt.datetime.utcnow().date()

    for d, df in enumerate([teams_df, players_df, events_df, fixtures_df]):
        df['date_created'] = today.isoformat()
        df['current_event'] = blob_data_json['current-event']
        df.to_gbq(f"{table_names[d]}.{today.year}_event_{blob_data_json['current-event']}",
                  project_id=FPL_PROJECT_ID, if_exists='replace')

    logging.info('Wrote dataframes to GBQ')
    return True
