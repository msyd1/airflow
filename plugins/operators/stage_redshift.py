from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    json_copy_task = '''COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}' 
    SECRET_ACCESS_KEY '{}' 
    JSON '{}'
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 json="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format=file_format
        self.aws_credentials_id = aws_credentials_id
        self.json = json

    def execute(self, context):
        # Parameters:
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Deleting old table
        self.log.info("Old table: deleting")
        redshift.run("DELETE FROM {}".format(self.table))
        
        # Loading data:
        self.log.info("Loading tables from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.json_copy_task.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json
            )
                         
        redshift.run(formatted_sql)
        self.log.info(f"{self.table} table: Loaded")
