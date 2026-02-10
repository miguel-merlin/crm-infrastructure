import boto3
import os
from collections import Counter, defaultdict
from botocore.exceptions import ClientError

def scan_table(table):
    """
    Scans a DynamoDB table handling pagination.
    Returns a list of all items.
    """
    items = []
    try:
        response = table.scan()
        items.extend(response.get("Items", []))
        
        while "LastEvaluatedKey" in response:
            print(f"Scanning {table.name} more items... (Current count: {len(items)})")
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))
            
        print(f"Total records scanned from {table.name}: {len(items)}")
        return items
    except ClientError as e:
        print(f"Error scanning table {table.name}: {e}")
        return []

def get_metrics():
    responses_table_name = os.getenv("TABLE_NAME", "crm-api-responses")
    transactions_table_name = os.getenv("TRANSACTIONS_TABLE_NAME", "crm-quotes-emails-transactions")
    
    region = os.getenv("AWS_REGION", "us-west-1") # Defaulting to us-east-1 as seen in the stack code
    profile = os.getenv("AWS_PROFILE", "hidrorey")
    
    print(f"Connecting to DynamoDB tables using profile: {profile} in region: {region}...")
    
    session = boto3.Session(profile_name=profile)
    dynamodb = session.resource("dynamodb", region_name=region)
    
    responses_table = dynamodb.Table(responses_table_name)
    transactions_table = dynamodb.Table(transactions_table_name)
    
    try:
        # 1. Scan Transactions
        print(f"Scanning {transactions_table_name}...")
        transactions = scan_table(transactions_table)
        
        # Process Transactions
        txn_map = {} # txn_id -> sales_rep_name
        quotes_sent_per_rep = Counter()
        total_quotes_sent = 0
        
        for txn in transactions:
            total_quotes_sent += 1
            txn_id = txn.get('transaction_id')
            sales_rep = txn.get('sales_rep', {})
            
            # Handle case where sales_rep might be just a string (if legacy) or dict
            if isinstance(sales_rep, dict):
                rep_name = sales_rep.get('name', 'Unknown')
            else:
                rep_name = str(sales_rep)
            
            if txn_id:
                txn_map[txn_id] = rep_name
            quotes_sent_per_rep[rep_name] += 1
            
        # 2. Scan Responses
        print(f"Scanning {responses_table_name}...")
        responses = scan_table(responses_table)
        
        # Process Responses
        response_metrics = Counter()
        rep_response_metrics = defaultdict(Counter)
        
        for item in responses:
            response_type = item.get("response_type")
            txn_id = item.get("email_transaction_id")
            
            if response_type:
                response_metrics[response_type] += 1
                
                # Link to Sales Rep
                rep_name = txn_map.get(txn_id, "Unknown")
                rep_response_metrics[rep_name][response_type] += 1
                
        # 3. Print Report
        print("\n" + "="*40)
        print("       CRM METRICS REPORT")
        print("="*40)
        
        print(f"\nTotal Quotes Sent: {total_quotes_sent}")
        
        print("\n--- Global Response Metrics ---")
        if response_metrics:
            for r_type, count in response_metrics.items():
                print(f"{r_type}: {count}")
        else:
            print("No responses found.")
        
        print("\n--- Per Sales Rep Metrics ---")
        # Get all unique reps
        all_reps = set(quotes_sent_per_rep.keys()) | set(rep_response_metrics.keys())
        
        if all_reps:
            for rep in sorted(all_reps):
                print(f"\nSales Rep: {rep}")
                print(f"  Quotes Sent: {quotes_sent_per_rep.get(rep, 0)}")
                print(f"  Responses:")
                if rep in rep_response_metrics:
                    for r_type, count in rep_response_metrics[rep].items():
                        print(f"    {r_type}: {count}")
                else:
                    print("    (None)")
        else:
            print("No sales rep data found.")
            
        print("\n" + "="*40)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    get_metrics()
