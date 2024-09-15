import snowflake.connector

envm='DEV'
user='DEV_DATA_LOADER_USER'
password='BrTh4sndUcu3sY'
account='bv18165.central-us.azure'
raw_database='DEV_RAW_DB'
raw_schema='IAUDITOR_RAW'
trf_database='DEV_TRF_DB'
trf_schema='IAUDITOR_TRF'
warehouse='DEV_DATA_LOADER_WH'
role='AR_DEV_ALL_ETL'

# Snowflake connection setup
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=raw_database,
    schema=raw_schema,
    role=role
)

try:
    # Reading the SQL script from the file
    with open('inspections-flatten.sql', 'r') as file:
        sql_query = file.read()

    # Executing the SQL script
    with conn.cursor() as cur:
        cur.execute(sql_query)

    # Logging the success
    print('SQL script executed successfully')

except Exception as e:
    print(f'Error executing the SQL script - Error: {e}')

# Closing the connection
conn.close()