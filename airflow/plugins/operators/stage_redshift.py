from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    
    redshift_sql = """ 
    COPY {} 
    FROM '{}' 
    ACCESS_KEY_ID '{}' 
    SECRET_ACCESS_KEY '{}'
    {}
    ;
    """
    @apply_defaults
    def __init__(self,
                 redshift_id = "",
                 aws_id = "",
                 schema ="",
                 s3_path = "",
                 s3_key = "",
                 query_end = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Mapping parameters here:
        self.redshift_id = redshift_id
        self.aws_id = aws_id
        self.schema = schema
        self.s3_path = s3_path
        self.s3_key = s3_key
        self.query_end = query_end

    def execute(self, context):
        aws_hook = AwsHook(self.aws_id)
        aws_cred = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_id)
        
        self.log.info("Clearing table")
        redshift_hook.run("DELETE FROM {}".format(self.schema))
        
        self.log.info("Copying data from S3 bucket to Redshift table")
        s3_conn_key = self.s3_key.format(**context)
        s3_bucket = "s3://{}/{}".format(self.s3_path, s3_conn_key)
        
        sql_query = StageToRedshiftOperator.redshift_sql.format(
            self.schema,
            s3_bucket,
            aws_cred.access_key,
            aws_cred.secret_key,
            self.query_end)
                      
        self.log.info("Running SQL query")
        redshift_hook.run(sql_query)





