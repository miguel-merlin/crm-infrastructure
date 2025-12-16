import unittest
import os
from src.utils import read_products_from_csv
from src.model import Product


class TestUtils(unittest.TestCase):
    def test_read_products_from_csv(self):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_path = os.path.join(base_dir, "test", "data", "products.csv")

        products = read_products_from_csv(csv_path)

        self.assertIsInstance(products, list)
        self.assertTrue(len(products) > 0, "No products were read from the CSV")
        self.assertIsInstance(products[0], Product)
        target_id = "1070976"
        found_product = next((p for p in products if p.id == target_id), Product())

        self.assertIsNotNone(found_product, f"Product with ID {target_id} not found")
        self.assertEqual(found_product.description, "HOSE, COOLANT, UPPER, MOLD")
        self.assertEqual(found_product.product_type, "REFACCIONES")


if __name__ == "__main__":
    unittest.main()
