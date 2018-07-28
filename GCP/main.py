"""Defines a number of GCP Python functions to be deployed.

Currently includes:
1. Database update function (runs on a bi-weekly CRON job)
"""
import requests as rq
from flask import Request

import logging
import datetime as dt
from io import BytesIO

FPL_BUCKET_NAME = 'fpl_fun'


def get_fpl_data(req: Request):
    """Cloud function to save data from FPL website.

    Writes JSON to Google Cloud storage.

    Args:
      req: Flask Request automatically passed by GCP Functions. Is ignored.

    Returns:
      Filename that was written to Google Cloud storage

    """

    logging.info("Begining FPL data collection run.")

    response = rq.get('https://fantasy.premierleague.com/drf/bootstrap-static')
    fpl_blob_name = 'fpl_data_' + dt.datetime.utcnow().isoformat()

    print(fpl_blob_name)

    from gcloud import storage

    storage_client = storage.Client()
    fpl_bucket = storage_client.bucket(FPL_BUCKET_NAME)

    new_blob = fpl_bucket.blob(fpl_blob_name)
    new_blob.upload_from_file(BytesIO(response.content), size=2048)

    logging.info(f"Data uploaded to {new_blob.name}.")

    return new_blob.name
