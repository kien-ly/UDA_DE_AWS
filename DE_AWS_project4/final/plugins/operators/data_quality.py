from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    has_rows = 'SELECT COUNT(*) FROM {};'

    @apply_defaults
    def __init__(self,
                 tables,
                 redshift_conn_id,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            self.log.info("Checking data quality...")
            self.log.info(f"Checking {table} table...")

            count = redshift.get_records(self.has_rows.format(table))
            if len(count) < 1 or len(count[0]) < 1 or count[0][0] < 1:
                raise ValueError(f"{table} contains 0 rows")