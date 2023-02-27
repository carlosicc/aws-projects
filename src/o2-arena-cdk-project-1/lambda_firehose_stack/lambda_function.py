import base64
import sys
import os
import json
import pymysql
import logging
import traceback
import boto3
from botocore.exceptions import ClientError

"""Important:
For homogeneous migration, i.e. MySQL to MySQL db, native tools can be more effective; e.g. mysqldump
"""

# Logger settings
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secret(secret_name, region_name):
    # Call Secrets Manager client, using the Lambda OS env vars:
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # Fetching password:
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            secret_payload = json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            secret_payload = json.loads('{"message": "no data found"}')

        return secret_payload

    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e


# Get database credentials, from AWS Secrets Manager
os_secret_name = os.environ['db_secret_name']
os_region_name = os.environ['region_name']
credentials_secret = get_secret(secret_name=os_secret_name, region_name=os_region_name)

# Use credentials
mysql_host  = credentials_secret['mysql_host']
db_name = credentials_secret['mysql_db_name']
username = credentials_secret['mysql_db_user']
password = credentials_secret['mysql_db_password']


def lambda_handler(event, context):
    for record in event['records']:   
        try:
            # The data blob, which is base64-encoded when the blob is serialized.
            payload = base64.b64decode(record['data']).decode('utf-8')

            # Easier to work with dict
            row = json.loads(payload)

            # Connect to DB
            conn = pymysql.connect(host=mysql_host, user=username, passwd=password, db=db_name, connect_timeout=5)

            # Write to DB
            with conn.cursor() as cur:
                logger.info('INFO: inserting row {}'.format(row))

                # Build and run DML command (consider SQL Injection)
                dml_stmt = 'insert into tbl_smart_motion_model_x (device_id, ts, latitude, longitude, motion_detected, device_status) values(%s, %s, %s, %s, %s, %s)'
                response = cur.execute(dml_stmt, (row['_id_'], row['ts'], row['geo_coordinates']['latitude'], row['geo_coordinates']['longitude'], row['motion_detected'], row['device_status']))
                
                # Optional: logger.info('INFO: {}'.format(response))
                conn.commit()

            return {
                    'statusCode': 200,
                    'body': json.dumps('Data written successfully to MySQL')
                    }
        
        except pymysql.MySQLError as e:
            logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
            logger.error(e)
            sys.exit()

        except Exception as e:
            logger.error("ERROR: Connection to DB succeeded, but something else went wrong:")
            traceback.print_exc()
