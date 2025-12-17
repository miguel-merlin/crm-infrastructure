import unittest
from unittest.mock import MagicMock, patch
import os
import sys

# Add the lambda directory to the python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import parse_s3_event, download_file_from_s3, handler
from model import SalesRep


class TestMain(unittest.TestCase):
    def test_parse_s3_event_success(self):
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "test-key.csv"},
                    }
                }
            ]
        }
        bucket, key = parse_s3_event(event)
        self.assertEqual(bucket, "test-bucket")
        self.assertEqual(key, "test-key.csv")

    def test_parse_s3_event_failure(self):
        event = {}
        with self.assertRaises(ValueError):
            parse_s3_event(event)

    @patch("main.tempfile.NamedTemporaryFile")
    def test_download_file_from_s3(self, mock_tempfile):
        mock_s3 = MagicMock()
        mock_file = MagicMock()
        mock_file.name = "/tmp/test.csv"
        mock_tempfile.return_value.__enter__.return_value = mock_file

        path = download_file_from_s3(mock_s3, "bucket", "key")

        self.assertEqual(path, "/tmp/test.csv")
        mock_s3.download_fileobj.assert_called_once()

    @patch("main.boto3.client")
    @patch("main.boto3.resource")
    @patch("main.safe_get_env")
    @patch("main.parse_s3_event")
    @patch("main.download_file_from_s3")
    @patch("main.read_sales_reps_from_csv")
    @patch("main.write_sales_reps_to_dynamo")
    @patch("main.os.path.exists")
    @patch("main.os.unlink")
    def test_handler_success(
        self,
        mock_unlink,
        mock_exists,
        mock_write_dynamo,
        mock_read_csv,
        mock_download,
        mock_parse,
        mock_get_env,
        mock_resource,
        mock_client,
    ):
        # Setup mocks
        mock_get_env.return_value = "TestTable"
        mock_parse.return_value = ("bucket", "key")
        mock_download.return_value = "/tmp/file.csv"
        mock_read_csv.return_value = [
            SalesRep("1", "John", "john@example.com")
        ]
        mock_write_dynamo.return_value = {
            "successful_inserts": 1,
            "failed_inserts": 0,
        }
        mock_exists.return_value = True

        event = {}
        context = {}

        response = handler(event, context)

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(response["body"]["total"], 1)
        self.assertEqual(response["body"]["successful_inserts"], 1)
        
        # Verify clean up happened
        mock_unlink.assert_called_with("/tmp/file.csv")

    @patch("main.boto3.client")
    @patch("main.boto3.resource")
    @patch("main.safe_get_env")
    @patch("main.parse_s3_event")
    def test_handler_event_error(
        self, mock_parse, mock_get_env, mock_resource, mock_client
    ):
        mock_parse.side_effect = ValueError("Invalid event")
        
        response = handler({}, {})
        
        self.assertEqual(response["statusCode"], 400)
        self.assertIn("Invalid event structure", response["body"]["error"])

    @patch("main.boto3.client")
    @patch("main.boto3.resource")
    @patch("main.safe_get_env")
    @patch("main.parse_s3_event")
    @patch("main.download_file_from_s3")
    @patch("main.os.path.exists")
    @patch("main.os.unlink")
    def test_handler_download_error(
        self,
        mock_unlink,
        mock_exists,
        mock_download,
        mock_parse,
        mock_get_env,
        mock_resource,
        mock_client,
    ):
        mock_parse.return_value = ("bucket", "key")
        mock_download.side_effect = Exception("S3 Error")
        # Simulate that file was created before error, so we test cleanup
        mock_exists.return_value = True 
        
        # We need to ensure download_file_from_s3 returns something or side_effect triggers.
        # Here side_effect triggers, so temp_file_path is None in handler init, 
        # but wait, handler initializes temp_file_path = None.
        # If download fails, temp_file_path is not assigned the result. 
        # The exception handler checks `if temp_file_path and ...`
        # So we can't test unlink here unless we mock the local variable which is hard.
        # Actually, if download fails inside `download_file_from_s3`, that function handles the temp file creation.
        # But `handler` catches the exception.
        # If `download_file_from_s3` fails, it raises exception. `temp_file_path` in handler remains `None`.
        # So unlink won't be called in the exception block.
        
        response = handler({}, {})
        
        self.assertEqual(response["statusCode"], 500)
        self.assertIn("S3 Error", response["body"]["error"])


if __name__ == "__main__":
    unittest.main()
