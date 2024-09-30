import boto3
import psycopg2
import os

# Initialize boto3 clients for Glue and Redshift
glue_client = boto3.client('glue')
redshift_client = boto3.client('redshift')

# Environment variables (set in Lambda function configuration)
GLUE_DB_NAME = os.getenv('GLUE_DB_NAME')  # Name of the Glue database
EXTERNAL_SCHEMA_NAME = os.getenv('EXTERNAL_SCHEMA_NAME')  # Name of the schema to create in Redshift
REDSHIFT_CLUSTER_ID = os.getenv('REDSHIFT_CLUSTER_ID')  # Redshift cluster identifier
REDSHIFT_DATABASE = os.getenv('REDSHIFT_DATABASE')  # Redshift database name
REDSHIFT_USER = os.getenv('REDSHIFT_USER')  # Redshift user name
REDSHIFT_SECRET_ARN = os.getenv('REDSHIFT_SECRET_ARN')  # Secrets Manager ARN for Redshift credentials

# Lambda handler
def lambda_handler(event, context):
    try:
        # Get Redshift cluster credentials (via Secrets Manager or hard-coded)
        credentials = boto3.client('secretsmanager').get_secret_value(SecretId=REDSHIFT_SECRET_ARN)
        redshift_password = credentials['SecretString']
        
        # Fetch Redshift cluster information
        cluster_info = redshift_client.describe_clusters(ClusterIdentifier=REDSHIFT_CLUSTER_ID)
        redshift_endpoint = cluster_info['Clusters'][0]['Endpoint']['Address']

        # Connect to Redshift
        conn = psycopg2.connect(
            host=redshift_endpoint,
            port=5439,
            user=REDSHIFT_USER,
            password=redshift_password,
            dbname=REDSHIFT_DATABASE
        )
        cursor = conn.cursor()

        # Create external schema in Redshift
        create_external_schema(cursor)

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print(f"Error: {e}")
        raise e

    finally:
        # Clean up
        if conn:
            cursor.close()
            conn.close()

def create_external_schema(cursor):
    glue_role_arn = get_glue_role_arn()
    
    sql = f"""
    CREATE EXTERNAL SCHEMA {EXTERNAL_SCHEMA_NAME}
    FROM DATA CATALOG
    DATABASE '{GLUE_DB_NAME}'
    IAM_ROLE '{glue_role_arn}'
    """
    
    print(f"Executing SQL:\n{sql}")
    cursor.execute(sql)

def get_glue_role_arn():
    """Fetch the IAM role associated with Glue Data Catalog access"""
    # If you have a specific role, you can hardcode or pass it via environment variables.
    return os.getenv('GLUE_ROLE_ARN')

