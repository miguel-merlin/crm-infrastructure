import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

import utils
from model import SalesRep

DATA_FILE = Path(__file__).parent / "data" / "sales_rep.csv"


def test_get_sales_rep_from_csv_returns_matching_rep():
    sales_rep = utils.get_sales_rep_from_csv(str(DATA_FILE), "8")

    assert sales_rep == SalesRep(
        id="8",
        name="JORGE RODRIGUEZ",
        email="ventasweb@hidrorey.mx"
    )


def test_get_sales_rep_from_csv_returns_empty_when_not_found():
    sales_rep = utils.get_sales_rep_from_csv(str(DATA_FILE), "999")

    assert sales_rep == SalesRep()
