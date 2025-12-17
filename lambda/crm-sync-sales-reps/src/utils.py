import os
import csv
from typing import List, Dict, Any
from model import SalesRep


def safe_get_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise EnvironmentError(f"Environment variable '{var_name}' is not set.")
    return value


def read_sales_reps_from_csv(file_path: str) -> List[SalesRep]:
    """
    Retrieves all sales reps from a CSV file that contains AGENTE, NOMBRE, EMAIL, TEL columns.
    Returns a list of SalesRep objects.
    """
    sales_reps: List[SalesRep] = []

    try:
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
    except FileNotFoundError:
        return []

    return sales_reps


def write_sales_reps_to_dynamo(table: Any, sales_reps: List[SalesRep]) -> Dict[str, int]:
    success_count = 0
    error_count = 0
    with table.batch_writer() as batch:
        for sales_rep in sales_reps:
            try:
                batch.put_item(Item=sales_rep.to_dynamo_item())
                success_count += 1
            except Exception as e:
                print(f"Error inserting sales rep {sales_rep.id}: {e}")
                error_count += 1
    
    return {
        "successful_inserts": success_count,
        "failed_inserts": error_count,
    }
