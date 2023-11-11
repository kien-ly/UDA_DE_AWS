from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 query,
                 redshift_conn_id,
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.query = query
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Creating table...')
        self.log.info(self.query)
        redshift.run(self.query)