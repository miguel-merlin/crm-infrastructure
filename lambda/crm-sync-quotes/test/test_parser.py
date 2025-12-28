import unittest
from parser import QuoteParser

class TestQuoteParser(unittest.TestCase):
    def setUp(self):
        # We don't need real files for testing internal methods
        self.parser = QuoteParser("fake.zip", "assets/sales_rep.csv")

    def test_group_items_by_quote(self):
        cotizad_records = [
            {"NO_COT": 100.0, "CVE_PROD": "PROD1"},
            {"NO_COT": 100.0, "CVE_PROD": "PROD2"},
            {"NO_COT": 101.0, "CVE_PROD": "PROD3"},
        ]
        
        result = self.parser._group_items_by_quote(cotizad_records)
        
        self.assertEqual(len(result), 2)
        self.assertIn("100", result)
        self.assertIn("101", result)
        self.assertEqual(result["100"], ["PROD1", "PROD2"])
        self.assertEqual(result["101"], ["PROD3"])

if __name__ == "__main__":
    unittest.main()
