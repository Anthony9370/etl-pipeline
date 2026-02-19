# ETL configuration file for Postgres and BigQuery

POSTGRES = {
    'host': 'localhost',  # Replace with your actual host
    'port': 5432,
    'database': 'YOUR_DATABASE_NAME',  # Replace with your actual database name
    'user': 'YOUR_USERNAME',  # Replace with your actual username
    'password': 'YOUR_PASSWORD'  # Replace with your actual password,
}

BIGQUERY = {
    'project': 'HomePC',  # Replace with your actual GCP project ID
    'dataset': 'your_dataset',  # Replace with your actual BigQuery dataset name
    # Path to your GCP service account key file
    'credentials': 'path/to/your/service_account_key.json'  # Replace with the actual path to your service account key file,
}

# List of tables to extract
TABLES = [
    'Payment',
    'Subscription',
    'Parish',
    'User',
    'Plan',
    'Invoice',
    # Add more if needed
]

