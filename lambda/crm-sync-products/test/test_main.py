import unittest
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock
from main import parse_s3_event, download_file_from_s3, handler
from model import Product, DBWriteResult
from utils import safe_get_env, read_products_from_csv, write_products_to_dynamo


class TestParseS3Event(unittest.TestCase):
    """Tests for parse_s3_event function"""

    def test_parse_valid_s3_event(self):
        """Test parsing a valid S3 event"""
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "test-folder/file.csv"},
                    }
                }
            ]
        }

        bucket_name, object_key = parse_s3_event(event)

        self.assertEqual(bucket_name, "test-bucket")
        self.assertEqual(object_key, "test-folder/file.csv")

    def test_parse_event_missing_records(self):
        """Test parsing event with missing Records key"""
        event = {}

        with self.assertRaises(ValueError) as context:
            parse_s3_event(event)
        self.assertIn("Error parsing event", str(context.exception))

    def test_parse_event_empty_records(self):
        """Test parsing event with empty Records array"""
        event = {"Records": []}

        with self.assertRaises(ValueError) as context:
            parse_s3_event(event)
        self.assertIn("Error parsing event", str(context.exception))

    def test_parse_event_missing_bucket_name(self):
        """Test parsing event with missing bucket name"""
        event = {"Records": [{"s3": {"object": {"key": "file.csv"}}}]}

        with self.assertRaises(ValueError) as context:
            parse_s3_event(event)
        self.assertIn("Error parsing event", str(context.exception))

    def test_parse_event_missing_object_key(self):
        """Test parsing event with missing object key"""
        event = {"Records": [{"s3": {"bucket": {"name": "test-bucket"}}}]}

        with self.assertRaises(ValueError) as context:
            parse_s3_event(event)
        self.assertIn("Error parsing event", str(context.exception))


class TestDownloadFileFromS3(unittest.TestCase):
    """Tests for download_file_from_s3 function"""

    def test_download_file_successfully(self):
        """Test successful file download from S3"""
        mock_s3_client = Mock()
        bucket_name = "test-bucket"
        object_key = "test.csv"

        temp_file_path = download_file_from_s3(mock_s3_client, bucket_name, object_key)

        # Verify download_fileobj was called with correct parameters
        mock_s3_client.download_fileobj.assert_called_once()
        call_args = mock_s3_client.download_fileobj.call_args
        self.assertEqual(call_args[0][0], bucket_name)
        self.assertEqual(call_args[0][1], object_key)

        # Verify file was created
        self.assertTrue(temp_file_path.endswith(".csv"))
        self.assertTrue(os.path.exists(temp_file_path))

        # Cleanup
        os.unlink(temp_file_path)

    def test_download_file_s3_error(self):
        """Test handling of S3 download error"""
        mock_s3_client = Mock()
        mock_s3_client.download_fileobj.side_effect = Exception("S3 Error")

        with self.assertRaises(Exception) as context:
            download_file_from_s3(mock_s3_client, "bucket", "key")
        self.assertIn("S3 Error", str(context.exception))


class TestSafeGetEnv(unittest.TestCase):
    """Tests for safe_get_env function"""

    def test_get_existing_env_variable(self):
        """Test getting an existing environment variable"""
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            result = safe_get_env("TEST_VAR")
            self.assertEqual(result, "test_value")

    def test_get_missing_env_variable(self):
        """Test getting a missing environment variable"""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(EnvironmentError) as context:
                safe_get_env("MISSING_VAR")
            self.assertIn(
                "Environment variable 'MISSING_VAR' is not set", str(context.exception)
            )

    def test_get_empty_env_variable(self):
        """Test getting an empty environment variable"""
        with patch.dict(os.environ, {"EMPTY_VAR": ""}):
            with self.assertRaises(EnvironmentError) as context:
                safe_get_env("EMPTY_VAR")
            self.assertIn(
                "Environment variable 'EMPTY_VAR' is not set", str(context.exception)
            )


class TestReadProductsFromCSV(unittest.TestCase):
    """Tests for read_products_from_csv function"""

    def test_read_valid_csv(self):
        """Test reading a valid CSV file"""
        csv_content = """col1,col2,col3,Clave,Description,col6,col7,col8,col9,col10,col11,col12,col13,col14,ProductType
data1,data2,data3,PROD001,Product 1,d6,d7,d8,d9,d10,d11,d12,d13,d14,Type A
data1,data2,data3,PROD002,Product 2,d6,d7,d8,d9,d10,d11,d12,d13,d14,Type B"""

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".csv", encoding="latin-1"
        ) as f:
            f.write(csv_content)
            temp_file = f.name

        try:
            products = read_products_from_csv(temp_file)

            self.assertEqual(len(products), 2)
            self.assertEqual(products[0].id, "PROD001")
            self.assertEqual(products[0].description, "Product 1")
            self.assertEqual(products[0].product_type, "Type A")
            self.assertEqual(products[1].id, "PROD002")
            self.assertEqual(products[1].description, "Product 2")
            self.assertEqual(products[1].product_type, "Type B")
        finally:
            os.unlink(temp_file)

    def test_read_csv_without_header(self):
        """Test reading CSV without proper header"""
        csv_content = """col1,col2,col3,col4
data1,data2,data3,data4"""

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".csv", encoding="latin-1"
        ) as f:
            f.write(csv_content)
            temp_file = f.name

        try:
            products = read_products_from_csv(temp_file)
            self.assertEqual(len(products), 0)
        finally:
            os.unlink(temp_file)

    def test_read_csv_with_empty_rows(self):
        """Test reading CSV with empty rows"""
        csv_content = """col1,col2,col3,Clave,Description,col6,col7,col8,col9,col10,col11,col12,col13,col14,ProductType

data1,data2,data3,PROD001,Product 1,d6,d7,d8,d9,d10,d11,d12,d13,d14,Type A

data1,data2,data3,PROD002,Product 2,d6,d7,d8,d9,d10,d11,d12,d13,d14,Type B"""

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".csv", encoding="latin-1"
        ) as f:
            f.write(csv_content)
            temp_file = f.name

        try:
            products = read_products_from_csv(temp_file)
            self.assertEqual(len(products), 2)
        finally:
            os.unlink(temp_file)

    def test_read_csv_with_short_rows(self):
        """Test reading CSV with rows that have fewer than 15 columns"""
        csv_content = """col1,col2,col3,Clave,Description,col6,col7,col8,col9,col10,col11,col12,col13,col14,ProductType
data1,data2,data3,PROD001
data1,data2,data3,PROD002,Product 2,d6,d7,d8,d9,d10,d11,d12,d13,d14,Type B"""

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".csv", encoding="latin-1"
        ) as f:
            f.write(csv_content)
            temp_file = f.name

        try:
            products = read_products_from_csv(temp_file)
            self.assertEqual(len(products), 1)
            self.assertEqual(products[0].id, "PROD002")
        finally:
            os.unlink(temp_file)

    def test_read_csv_with_empty_id(self):
        """Test reading CSV with empty ID field"""
        csv_content = """col1,col2,col3,Clave,Description,col6,col7,col8,col9,col10,col11,col12,col13,col14,ProductType
data1,data2,data3,,Product 1,d6,d7,d8,d9,d10,d11,d12,d13,d14,Type A
data1,data2,data3,PROD002,Product 2,d6,d7,d8,d9,d10,d11,d12,d13,d14,Type B"""

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".csv", encoding="latin-1"
        ) as f:
            f.write(csv_content)
            temp_file = f.name

        try:
            products = read_products_from_csv(temp_file)
            self.assertEqual(len(products), 1)
            self.assertEqual(products[0].id, "PROD002")
        finally:
            os.unlink(temp_file)

    def test_read_csv_strips_whitespace(self):
        """Test that CSV reader strips whitespace from fields"""
        csv_content = """col1,col2,col3,Clave,Description,col6,col7,col8,col9,col10,col11,col12,col13,col14,ProductType
data1,data2,data3,  PROD001  ,  Product 1  ,d6,d7,d8,d9,d10,d11,d12,d13,d14,  Type A  """

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".csv", encoding="latin-1"
        ) as f:
            f.write(csv_content)
            temp_file = f.name

        try:
            products = read_products_from_csv(temp_file)
            self.assertEqual(len(products), 1)
            self.assertEqual(products[0].id, "PROD001")
            self.assertEqual(products[0].description, "Product 1")
            self.assertEqual(products[0].product_type, "Type A")
        finally:
            os.unlink(temp_file)


class TestHandler(unittest.TestCase):
    """Tests for the main Lambda handler function"""

    def setUp(self):
        """Set up test fixtures"""
        self.valid_s3_event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "products.csv"},
                    }
                }
            ]
        }

        self.sample_products = [
            Product(id="PROD001", description="Product 1", product_type="Type A"),
            Product(id="PROD002", description="Product 2", product_type="Type B"),
        ]

    @patch("main.boto3")
    @patch("main.download_file_from_s3")
    @patch("main.read_products_from_csv")
    @patch("main.write_products_to_dynamo")
    @patch.dict(os.environ, {"TABLE_NAME": "test-table"})
    def test_handler_success(
        self, mock_write_products, mock_read_products, mock_download, mock_boto3
    ):
        """Test successful handler execution"""
        temp_file = "/tmp/test.csv"
        mock_download.return_value = temp_file
        mock_read_products.return_value = self.sample_products

        mock_write_result = DBWriteResult(successful_inserts=2, failed_inserts=0)
        mock_write_products.return_value = mock_write_result

        with patch("main.os.path.exists", return_value=True), patch(
            "main.os.unlink"
        ) as mock_unlink:

            result = handler(self.valid_s3_event, None)

        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(result["body"]["total"], 2)
        self.assertEqual(result["body"]["successful_inserts"], 2)
        self.assertEqual(result["body"]["failed_inserts"], 0)

        mock_download.assert_called_once()
        mock_read_products.assert_called_once_with(temp_file)
        mock_write_products.assert_called_once()
        mock_unlink.assert_called_once_with(temp_file)

    @patch("main.boto3")
    @patch.dict(os.environ, {"TABLE_NAME": "test-table"})
    def test_handler_invalid_event(self, mock_boto3):
        """Test handler with invalid event structure"""
        invalid_event = {"Records": []}

        result = handler(invalid_event, None)

        self.assertEqual(result["statusCode"], 400)
        self.assertIn("error", result["body"])
        self.assertEqual(result["body"]["error"], "Invalid event structure")

    @patch("main.boto3")
    @patch("main.download_file_from_s3")
    @patch.dict(os.environ, {"TABLE_NAME": "test-table"})
    def test_handler_download_error(self, mock_download, mock_boto3):
        """Test handler when S3 download fails"""
        mock_download.side_effect = Exception("Download failed")

        result = handler(self.valid_s3_event, None)

        self.assertEqual(result["statusCode"], 500)
        self.assertIn("error", result["body"])
        self.assertIn("Download failed", result["body"]["error"])

    @patch("main.boto3")
    @patch("main.download_file_from_s3")
    @patch("main.read_products_from_csv")
    @patch.dict(os.environ, {"TABLE_NAME": "test-table"})
    def test_handler_csv_read_error(
        self, mock_read_products, mock_download, mock_boto3
    ):
        """Test handler when CSV reading fails"""
        temp_file = "/tmp/test.csv"
        mock_download.return_value = temp_file
        mock_read_products.side_effect = Exception("CSV parsing error")

        with patch("main.os.path.exists", return_value=True), patch(
            "main.os.unlink"
        ) as mock_unlink:

            result = handler(self.valid_s3_event, None)

        self.assertEqual(result["statusCode"], 500)
        self.assertIn("error", result["body"])
        self.assertIn("CSV parsing error", result["body"]["error"])
        mock_unlink.assert_called_once_with(temp_file)

    @patch("main.boto3")
    @patch("main.download_file_from_s3")
    @patch("main.read_products_from_csv")
    @patch("main.write_products_to_dynamo")
    @patch.dict(os.environ, {"TABLE_NAME": "test-table"})
    def test_handler_dynamo_write_error(
        self, mock_write_products, mock_read_products, mock_download, mock_boto3
    ):
        """Test handler when DynamoDB write fails"""
        temp_file = "/tmp/test.csv"
        mock_download.return_value = temp_file
        mock_read_products.return_value = self.sample_products
        mock_write_products.side_effect = Exception("DynamoDB error")

        with patch("main.os.path.exists", return_value=True), patch(
            "main.os.unlink"
        ) as mock_unlink:

            result = handler(self.valid_s3_event, None)

        self.assertEqual(result["statusCode"], 500)
        self.assertIn("error", result["body"])
        self.assertIn("DynamoDB error", result["body"]["error"])
        mock_unlink.assert_called_once_with(temp_file)

    @patch("main.boto3")
    @patch("main.download_file_from_s3")
    @patch("main.read_products_from_csv")
    @patch("main.write_products_to_dynamo")
    @patch.dict(os.environ, {"TABLE_NAME": "test-table"})
    def test_handler_partial_success(
        self, mock_write_products, mock_read_products, mock_download, mock_boto3
    ):
        """Test handler with partial write success"""
        temp_file = "/tmp/test.csv"
        mock_download.return_value = temp_file
        mock_read_products.return_value = self.sample_products

        mock_write_result = DBWriteResult(successful_inserts=1, failed_inserts=1)
        mock_write_products.return_value = mock_write_result

        with patch("main.os.path.exists", return_value=True), patch("main.os.unlink"):

            result = handler(self.valid_s3_event, None)

        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(result["body"]["total"], 2)
        self.assertEqual(result["body"]["successful_inserts"], 1)
        self.assertEqual(result["body"]["failed_inserts"], 1)

    @patch("main.boto3")
    @patch("main.download_file_from_s3")
    @patch("main.read_products_from_csv")
    @patch("main.write_products_to_dynamo")
    @patch.dict(os.environ, {"TABLE_NAME": "test-table"})
    def test_handler_temp_file_cleanup(
        self, mock_write_products, mock_read_products, mock_download, mock_boto3
    ):
        """Test that temporary file is cleaned up even on error"""
        temp_file = "/tmp/test.csv"
        mock_download.return_value = temp_file
        mock_read_products.return_value = self.sample_products
        mock_write_products.side_effect = Exception("Write error")

        with patch("main.os.path.exists", return_value=True), patch(
            "main.os.unlink"
        ) as mock_unlink:

            handler(self.valid_s3_event, None)

            mock_unlink.assert_called_once_with(temp_file)


class TestProductModel(unittest.TestCase):
    """Tests for Product model"""

    def test_product_to_dynamo_item(self):
        """Test converting Product to DynamoDB item format"""
        product = Product(
            id="PROD001", description="Test Product", product_type="Type A"
        )

        dynamo_item = product.to_dynamo_item()

        self.assertEqual(dynamo_item["id"]["S"], "PROD001")
        self.assertEqual(dynamo_item["description"]["S"], "Test Product")
        self.assertEqual(dynamo_item["product_type"]["S"], "Type A")

    def test_product_default_values(self):
        """Test Product with default values"""
        product = Product()

        self.assertEqual(product.id, "")
        self.assertEqual(product.description, "")
        self.assertEqual(product.product_type, "")


if __name__ == "__main__":
    unittest.main()
