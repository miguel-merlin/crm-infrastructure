import csv
from typing import List
from model import SalesRep


def read_sales_reps_from_csv(file_path: str) -> List[SalesRep]:
    """
    Retrieves all sales reps from a CSV file that contains AGENTE, NOMBRE, EMAIL, TEL columns.
    Returns a list of SalesRep objects.
    """
    sales_reps: List[SalesRep] = []

    with open(file_path, mode="r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if not row:
                continue
            sales_reps.append(
                SalesRep(
                    id=(row.get("AGENTE") or "").strip(),
                    name=(row.get("NOMBRE") or "").strip(),
                    email=(row.get("EMAIL") or "").strip(),
                )
            )

    return sales_reps
