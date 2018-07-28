import unittest

from gcloud import storage
import google.auth
import googleapiclient.discovery

from GCP.main import FPL_BUCKET_NAME


class TestGCP(unittest.TestCase):
    def setUp(self):
        self.creds, self.project = google.auth.default()

    def test_storage_bucket(self):
        storage_service = storage.Client()
        self.assertIsInstance(
            storage_service.get_bucket(FPL_BUCKET_NAME),
            storage.bucket.Bucket
        )

    def test_cloud_functions(self):
        cloud_funcs_client = googleapiclient.discovery.build('cloudfunctions', 'v1', credentials=self.creds)
        r = cloud_funcs_client.projects().locations().functions().list(parent='projects/*/locations/*')
        print(r.to_json())


if __name__ == '__main__':
    unittest.main()
