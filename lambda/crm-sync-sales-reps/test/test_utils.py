import unittest
import os
from src.model import SalesRep
from src.utils import read_sales_reps_from_csv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestUtils(unittest.TestCase):
    def test_read_sales_rep_from_csv_returns_all_reps(self):
        csv_path = os.path.join(BASE_DIR, "test", "data", "sales_rep.csv")
        sales_reps = read_sales_reps_from_csv(csv_path)

        self.assertEqual(len(sales_reps), 4)
        self.assertEqual(
            sales_reps[0],
            SalesRep(id="1", name="MIGUEL M. IBARRA", email="miguel@hidrorey.mx"),
        )
        self.assertEqual(
            sales_reps[1],
            SalesRep(id="8", name="JORGE RODRIGUEZ", email="ventasweb@hidrorey.mx"),
        )
        self.assertEqual(
            sales_reps[2],
            SalesRep(id="42", name="GABRIEL TORRES", email="ventasenlinea@hidrorey.mx"),
        )
        self.assertEqual(
            sales_reps[3],
            SalesRep(id="51", name="DAVID VARGAS", email="david@hidrorey.mx"),
        )

    def test_read_sales_rep_from_csv_handles_unexisting_file(self):
        empty_file_path = os.path.join(BASE_DIR, "test", "data", "does_not_exist.csv")
        sales_reps = read_sales_reps_from_csv(str(empty_file_path))
        self.assertEqual(len(sales_reps), 0)


if __name__ == "__main__":
    unittest.main()
