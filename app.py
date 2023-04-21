import boto3
import time
import pandas  as pd

bucket_name = 'your_bucket_name'
file_path = "redfin_2023-04-20-18-17-37.csv"
object_name = "redfin/redfin_2023-04-20-18-17-37.csv"
database_name = "redfin"
table_name = "redfin_house_data"
output_bucket = bucket_name


def upload_csv_to_s3(bucket, file_path, object_name):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket, object_name)

def create_athena_database(database_name):
    athena = boto3.client('athena')
    query = f"CREATE DATABASE IF NOT EXISTS {database_name};"

    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://{output_bucket}/athena-results/'
        }
    )

# Create the Athena database
create_athena_database(database_name)

def create_athena_table(database, table, s3_location):
    athena = boto3.client('athena')
    query = f"""
    CREATE EXTERNAL TABLE {database}.{table} (
        sale_type STRING,
        sold_date STRING,
        property_type STRING,
        address STRING,
        city STRING,
        state_or_province STRING,
        zip_or_postal_code STRING,
        price BIGINT,
        beds INT,
        baths INT,
        location STRING,
        square_feet INT,
        lot_size INT,
        year_built INT,
        days_on_market INT,
        price_per_square_feet INT,
        hoa_per_month STRING,
        status STRING,
        next_open_house_start_time STRING,
        next_open_house_end_time STRING,
        url STRING,
        source STRING,
        mls STRING,
        favorite STRING,
        interested STRING,
        latitude DOUBLE,
        longitude DOUBLE
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
      'serialization.format' = ',',
      'field.delim' = ','
    ) LOCATION '{s3_location}'
    TBLPROPERTIES ('has_encrypted_data'='false', 'skip.header.line.count'='1');
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': f's3://{output_bucket}/athena-results/'
        }
    )

def run_athena_query(database, query):
    athena = boto3.client('athena')
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': f's3://{output_bucket}/athena-results/'
        }
    )
    return response['QueryExecutionId']


def wait_for_query_to_complete(athena, query_execution_id, wait_interval=5):
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']

        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break

        time.sleep(wait_interval)

    if state == 'FAILED':
        error_message = response['QueryExecution']['Status'].get('StateChangeReason')
        print(f"Query failed: {error_message}")

    return state


def get_query_result(query_execution_id):
    athena = boto3.client('athena')
    wait_for_query_to_complete(athena, query_execution_id)
    response = athena.get_query_results(
        QueryExecutionId=query_execution_id
    )
    return response


# Set your parameters

# Upload the CSV file to S3
upload_csv_to_s3(bucket_name, file_path, object_name)

# Create the Athena table
create_athena_table(database_name, table_name, f's3://{bucket_name}/{object_name.rsplit("/", 1)[0]}/')

# Run an Athena query
query = """
SELECT AVG(price) as average_price
FROM redfin_house_data
WHERE property_type = 'Single Family Residential';
"""
query_execution_id = run_athena_query(database_name, query)

def format_athena_output(result):
    column_names = [column_info['Label'] for column_info in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    rows = result['ResultSet']['Rows'][1:]  # Exclude the header row

    formatted_rows = []
    for row in rows:
        formatted_row = [item['VarCharValue'] for item in row['Data']]
        formatted_rows.append(formatted_row)

    formatted_result = pd.DataFrame(formatted_rows, columns=column_names)
    return formatted_result


# Get the query result
result = get_query_result(query_execution_id)
formatted_result = format_athena_output(result)
print("Athena query result:")
print(formatted_result)


# Read the CSV file into a pandas DataFrame
data = pd.read_csv(file_path)

# Filter the data to only include rows with 'Single Family Residential' property type
filtered_data = data[data['PROPERTY TYPE'] == 'Single Family Residential']

# Calculate the average price of the filtered data
average_price = filtered_data['PRICE'].mean()
print('-' * 80)
print("Average price of Single Family Residential properties:", average_price)
