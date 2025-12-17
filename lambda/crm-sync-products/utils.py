import csv
from typing import List
from model import Product

def read_products_from_csv(file_path: str) -> List[Product]:
    """
    Reads products from a CSV file.
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
