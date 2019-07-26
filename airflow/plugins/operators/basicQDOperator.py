from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy import create_engine
import pandas as pd
from pandas.io.json import json_normalize
import quandl as qdl


class LoadQuandlOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn = redshift_conn
        #self.sql = sql
        self.table = table
        self.quandl_api = quandl_conn
        self.req_table = req_table

    def execute(self, context):
        self.log.info('Beginning implementation of Quandl-Redshift Load')
        hook_redshift = PostgresHook(self.redshift_conn)
        qdl.ApiConfig.api_key = qundl_conn
        raw_sentiments = qdl.get(self.req_table, paginate=True)
        filtered_sentiments = raw_sentiments.iloc[:, :-3]
        finished_sentiments = filtered_sentiments.reset_index()
        conn = create_engine(redshift_conn)
        finished_sentiments.to_sql(self.table, conn, index=False, if_exists = 'replace')
        self.log.info(f'Successfully loaded {self.req_table} data to {self.table}.')
            
