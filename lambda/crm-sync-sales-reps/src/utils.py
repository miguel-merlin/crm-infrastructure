import csv
from typing import List
from model import Product, SalesRep

def read_products_from_csv(file_path: str) -> List[Product]:
    """
    Reads products from a CSV file.
    Assumes specific column mapping based on the provided CSV format.
    """
    products = []
    with open(file_path, mode='r', encoding='latin-1') as csvfile:
        reader = csv.reader(csvfile)
        header_found = False
        for row in reader:
            if not row:
                continue
            if len(row) > 3 and row[3] == "Clave":
                header_found = True
                break
        
        if not header_found:
            return []
        for row in reader:
            if not row:
                continue
            if len(row) <= 14:
                continue
                
            id_ = row[3].strip()
            if not id_:
                continue
                
            description = row[4].strip()
            product_type = row[14].strip()
            
            products.append(Product(
                id=id_,
                description=description,
                product_type=product_type
            ))
            
    return products


def get_sales_rep_from_csv(file_path: str, sales_rep_id: str) -> SalesRep:
    """
    Retrieves a sales rep by id from a CSV file that contains AGENTE, NOMBRE, EMAIL, TEL columns.
    Returns an empty SalesRep if the id is not found.
    """
    target_id = str(sales_rep_id).strip()
    if not target_id:
        return SalesRep()

    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if not row:
                continue
            current_id = (row.get("AGENTE") or "").strip()
            if current_id != target_id:
                continue

            return SalesRep(
                id=current_id,
                name=(row.get("NOMBRE") or "").strip(),
                email=(row.get("EMAIL") or "").strip()
            )

    return SalesRep()
