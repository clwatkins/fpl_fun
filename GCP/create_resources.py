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

    try:
        os.remove('requirements.txt')
    except FileNotFoundError:
        pass

    completed_process = subprocess.run(
        f'pip freeze >> requirements.txt', shell=True
    )

    print('Requirements.txt created...')

    return completed_process


def create_get_fpl_data():
    """Deploys get_fpl_data function to GCP Functions.

    :return: CompletedProcess data type
    """
    completed_process = subprocess.run(
        f'gcloud beta functions deploy {main.get_fpl_data.__name__} --runtime=python37 --trigger-http '
        f'--source={os.path.dirname(main.__file__)} --project=fpl-fun --entry-point={main.get_fpl_data.__name__} '
        f'--env-vars-file .env.yaml',
    shell=True)

    return completed_process


def create_create_gcp_tables():
    """Deploys write_gcp_tables_pubsub function to GCP Functions.

    :return: CompletedProcess data type
    """

    completed_process = subprocess.run(
        f'gcloud beta functions deploy {main.write_gcp_tables_pubsub.__name__} --runtime=python37 '
        f'--source={os.path.dirname(main.__file__)} --project=fpl-fun '
        f'--entry-point={main.write_gcp_tables_pubsub.__name__} '
        f'--trigger-resource {main.FPL_BUCKET_NAME} --trigger-event google.storage.object.finalize '
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

    print(f'Bucket created {bucket_name}...')


if __name__ == '__main__':
    # create_requirements_txt()
    # print('\n')

    os.chdir(os.path.dirname(main.__file__))

    create_fpl_storage_bucket()
    print('\n')
    create_get_fpl_data()
    print('\n')
    create_create_gcp_tables()
