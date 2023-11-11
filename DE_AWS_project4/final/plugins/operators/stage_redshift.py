from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers.common_queries import CommonQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 table,
                 s3_path,
                 format_log,
                 aws_credentials_id,
                 redshift_conn_id,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_path = s3_path
        self.format_log = format_log
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Copy data to staging tables
        self.log.info(f"Copying Data from S3 Bucket to Redshift table {self.table}")
        copy_statement = CommonQueries.s3_copy.format(
            self.table,
            self.s3_path,
            aws_credentials[0],
            aws_credentials[1],
            self.format_log
        )
        self.log.info(copy_statement)
        redshift.run(copy_statement)

        