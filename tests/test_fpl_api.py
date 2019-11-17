import unittest
import json

from gcp.main import _fpl_downloader


class TestCloudFunctions(unittest.TestCase):

    def test_fpl_downloader(self):
        fpl_response = _fpl_downloader()
        fpl_json = json.loads(fpl_response)

        self.assertIsInstance(fpl_response, bytes)
        self.assertIn('teams', fpl_json.keys())


if __name__ == '__main__':
    unittest.main()
