import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

# AWS
AWS_ACCESS_KEY        = parser.get('aws', 'aws_access_key_id')
AWS_SECRET_KEY        = parser.get('aws', 'aws_secret_access_key')
AWS_SESSION_TOKEN_KEY = parser.get('aws', 'aws_session_token_key')
AWS_REGION            = parser.get('aws', 'aws_session_token_key')
AWS_S3_BUCKET         = parser.get('aws', 'aws_s3_bucket_name')
AWS_S3_TEMP_DIR       = parser.get('aws', 's3_data_directory')

# AWS Redshift
REDSHIFT_JDBC_URL = parser.get('aws', 'jdbc_url')
REDSHIFT_DB_NAME  = parser.get('aws', 'redshift_db_name')
REDSHIFT_DB_USER  = parser.get('aws', 'redshift_db_username')
REDSHIFT_DB_PASS  = parser.get('aws', 'redshift_db_pass')

# AWS IAM ROLE
AWS_IAM_ROLE = parser.get('aws', 'iam_role')