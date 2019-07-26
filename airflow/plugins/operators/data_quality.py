from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn = "",
                 tables = [],
                 sql_query="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn = redshift_conn
        #self.sql_query = sql_query
        self.tables = tables

    def execute(self, context):
        self.log.info('Implementing DataQualityOperator now.')
        hook_redshift = PostgresHook(self.redshift_conn)
        for table in self.tables:
            records = hook_redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"Table: {table} returned nothing.")
            num_records = records[0][0]
            if num_records == 0:
                self.log.error(f"{table} returned no records in destination.")
            self.log.info(f"Data validation check on {table} is completed.")