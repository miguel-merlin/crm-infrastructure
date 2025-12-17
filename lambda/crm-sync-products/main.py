from utils import read_products_from_csv

def handler(event, context):
    products = read_products_from_csv("../test/data/products.csv")
    print(f"Read {len(products)} products from CSV")
    for product in products:
        print(product)
