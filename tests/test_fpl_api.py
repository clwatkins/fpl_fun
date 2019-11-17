import unittest

from GCP.main import get_fpl_data


class TestCloudFunctions(unittest.TestCase):
    def setUp(self):
        pass

    def test_fpl_api_function(self):
        self.assertIsInstance(get_fpl_data(req=None), str)


if __name__ == '__main__':
    unittest.main()
