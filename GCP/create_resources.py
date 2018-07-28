import subprocess
import os

from GCP import main
from gcloud import storage
from gcloud.exceptions import Conflict


def create_requirements_txt():
    """Creates PIP requirements.txt file prior to deploying Cloud Functions.

    Sets CWD to the directory of main.py, then issues pip freeze terminal command.
    """

    os.chdir(os.path.dirname(main.__file__))

    completed_process = subprocess.run(
        f'pip freeze >> requirements.txt', shell=True
    )

    return completed_process


def create_get_fpl_data():
    completed_process = subprocess.run(
        f'gcloud beta functions deploy {main.get_fpl_data.__name__} --runtime=python37 --trigger-http '
        f'--source={os.path.dirname(main.__file__)} --project=fpl-fun --entry-point={main.get_fpl_data.__name__}'
        f'--env-vars-file .env.yaml',
    shell=True)

    return completed_process


def create_fpl_storage_bucket():
    """Definition of a simple Cloud Storage bucket.

    Documentation available here: https://cloud.google.com/storage/docs/reference/libraries
    """
    storage_client = storage.Client()
    try:
        fpl_bucket = storage_client.create_bucket(main.FPL_BUCKET_NAME)
        bucket_name = fpl_bucket.name
    except Conflict:
        bucket_name = main.FPL_BUCKET_NAME
        pass

    print(f'Bucket created {bucket_name}')


if __name__ == '__main__':
    create_fpl_storage_bucket()

    create_requirements_txt()
    create_get_fpl_data()
