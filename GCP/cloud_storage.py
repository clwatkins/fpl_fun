"""Definition of a simple Cloud Storage bucket.

Documentation available here: https://cloud.google.com/storage/docs/reference/libraries
"""
from gcloud import storage
from GCP.gcp_config import FPL_BUCKET_NAME


def create_fpl_storage_bucket():
    storage_client = storage.Client()
    fpl_bucket = storage_client.create_bucket(FPL_BUCKET_NAME)
    print(f'Bucket created {fpl_bucket.name}')


if __name__ == '__main__':
    create_fpl_storage_bucket()
