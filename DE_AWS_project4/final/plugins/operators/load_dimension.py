from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.common_queries import CommonQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 query,
                 table,
                 truncate,
                 redshift_conn_id,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.query = query
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f'Truncating dimension table...')
            redshift.run(CommonQueries.truncate_table.format(self.table))

        # Load data table
        self.log.info(f'Loading dimension table...')
        self.log.info(self.query)
        redshift.run(self.query)
