from utils import read_sales_reps_from_csv


def handler(event, context):
    sales_reps = read_sales_reps_from_csv("../test/data/sales_rep.csv")
    print(f"Read {len(sales_reps)} sales reps from CSV")
    for sales_rep in sales_reps:
        print(sales_rep)


if __name__ == "__main__":
    handler({}, {})
