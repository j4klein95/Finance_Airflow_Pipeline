from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
# from airflow.operators import DataQualityOperator
# from helpers import Quandl_Conf
# from helpers import SqlQueries
import quandl as qdl
import logging as log
import pandas as pd
from sqlalchemy import create_engine

#Create Default arguments to define behavior of the Dag.
default_args = {
    'owner': 'jay',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay' : timedelta(seconds=30),
    'catchup' : False,
    'email_on_retry' : False
}


#Setup and Load the Dag to Airflow
dag = DAG('finance_data_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

#Normally, I would put this information in Airflow itself, but since the Dag does not want
#   to read my plugins and helpers, I may need to setup a config file instead.
qdl.ApiConfig.api_key = Variable.get("quandl")
conn = create_engine(Variable.get("redshift"))
wt_key = Variable.get("wt_key")
rs_tables = ["us_stock_market_confidence_indices", "sp_500_composite_hist", "investment_sentiment"]

################### FUNCTIONS FOR THE DAG TO EXECUTE #####################################
def loadQuandl_sentiment(conn, redshift_table, **kwargs):
    """
    Function that goes out, pulls Investor Sentiment data, puts that data into a dataframe, loads directly into redshift
    
    Parameters:
    
    conn (Database Connection): A connection to the Redshift Database.
    
    redshift_table (str): The name of the table we are loading data too.
    
    **kwargs (kwargs): Needed for Python Operator in Airflow to set parameters.
    
    Returns:
    None
    
    """
    log.info('Beginning implementation of Quandl-Redshift Load.')
    
    #Make API call and feed data into a DataFrame, remove the last two columns
    raw_sentiments = qdl.get("AAII/AAII_SENTIMENT", paginate=True)
    filtered_sentiments = raw_sentiments.iloc[:, :-3]
    finished_sentiments = filtered_sentiments.reset_index()
    
    #Load data to RedShift 
    finished_sentiments.to_sql(redshift_table, conn, index=False, if_exists='replace')
    log.info(f'Successfully loaded Investor Sentiment data to {redshift_table}.')


def loadQuandl_yaleConf(conn, redshift_table, **kwargs):
    """
    Function that goes out, pulls indivdiual confidence data for valuation, crashes, and buy on dips. Combines into dataframe, and uploads to Redshift
    
    Parameters:
    
    conn (Database Connection): A connection to the Redshift Database.
    
    redshift_table (str): The name of the table we are loading data too.
    
    **kwargs (kwargs): Needed for Python Operator in Airflow to set parameters.
    
    Returns:
    None
    
    """
    log.info('Beginning implementation of Quandl-Redshift Load for Yale S&P Data.')
    
    #Make API call to create dataframes with confidence index data on valuations, crashes, and buy on dips. 
    raw_valuation_conf = qdl.get("YALE/US_CONF_INDEX_VAL_INDIV", paginate=True).reset_index()
    raw_crash_conf = qdl.get("YALE/US_CONF_INDEX_CRASH_INDIV", paginate=True).reset_index()
    raw_buy_dips = qdl.get("YALE/US_CONF_INDEX_BUY_INDIV", paginate=True).reset_index()
    
    #Map columns of dataframes to new names for better data clarity.
    val_conf = raw_valuation_conf.rename(columns={"Index Value": "Index_Value_Valuation_Confidence", "Standard Error": "Standard_Error_Valuation"})
    crash_conf = raw_crash_conf.rename(columns={"Index Value": "Index_Value_Crash_Confidence", "Standard Error": "Standard_Error_Crash"})
    bod_conf = raw_buy_dips.rename(columns={"Index Value": "Index_Value_Buy_on_Dip_Confidence", "Standard Error": "Standard_Error_Buy_on_Dip"})

    #Merge data into one final dataframe.
    confidence_ = pd.merge(val_conf, crash_conf, on="Date")
    confidence_final = pd.merge(confidence_, bod_conf, on="Date")
    
    #Send to Redshift
    confidence_final.to_sql(redshift_table, conn, index=False, if_exists='replace')
    log.info(f"Successfully loaded Individual's Confidence data to {redshift_table}.")
    
def loadQuandl_yaleComp(conn, redshift_table, **kwargs):
    """
    Function that goes out, pulls indivdiual confidence data for valuation, crashes, and buy on dips. Combines into dataframe, and uploads to Redshift
    
    Parameters:
    conn (Database Connection): A connection to the Redshift Database.
    
    redshift_table (str): The name of the table we are loading data too.
    
    **kwargs (kwargs): Needed for Python Operator in Airflow to set parameters.
    
    Returns:
    None
    
    """
    log.info('Beginning implementation of Quandl-Redshift Load for Yale S&P Data.')
    
    #Make API call to develop dataframe with S&P 500 composite data.
    raw_sp_comp = qdl.get("YALE/SPCOMP", paginate=True)
    finished_data = raw_sp_comp.reset_index()
    
    #Move data to Redshift         
    finished_data.to_sql(redshift_table, conn, index=False, if_exists='replace')
    log.info(f'Successfully loaded Yale S&P Composite data to {redshift_table}')
    
def get_ticker_data(wt_api_key, conn, table, **kwargs):
    """
    Makes API call to World Trading Data on a list of tickers. Grabs historical ticker data in JSON. Converts to dataframe. Loads to readshift.
    NOTE: There are limitations to the API. A smaller list of tickers are constructed for each call as is the date window.
    
    Parameters:
    conn (Database Connection): A connection to the Redshift Database.
    
    table (str): The name of the table we are loading data to.
    
    wt_api_key (str): The API key for making calls and gathering JSON data.
    
    **kwargs (kwargs): Needed for Python Operator in Airflow. 
    """
    #Input API Keys
    
    #Make list of ticker symbols
    ticker_df = pd.read_csv('data/WIKI_metadata.csv')
    ticker_lst = ticker_df['code']
    
    #Get List of Dates, utilize quandl API for specific dtes needed
    date_raw = qdl.get("AAII/AAII_SENTIMENT").reset_index()
    dates = date_raw['Date']
    date_lst = [date.strftime('%Y-%m-%d') for date in dates]
    
    raw_full_df = pd.DataFrame()
    
    #Loop through tickers and dates, append to dataframe
    for date in date_lst:
        for ticker in ticker_lst:
            url = (f"https://api.worldtradingdata.com/api/v1/history_multi_single_day?symbol={ticker}&date={date}&api_token={wt_api_key}")
            request = req.get(url).json()
            raw_df = pd.DataFrame(request)
            raw_full_df.append(raw_df)
            
    #Return a new processed dataframe
    processed_df = pd.concat([raw_full_df.drop(['data'], axis=1), raw_full_df['data'].apply(pd.Series)], axis=1).reset_index()
    
    #Load to RedShift
    processed_df.to_sql(table, conn, index=False, if_exists='replace')
    log.info(f'Successfully loaded ticker history to {table}')
    
def data_quality_check(redshift_conn, tables, **kwargs):
    """
    Does a quick data quality and validation check.
    
    Parameters:
    
    redshift_conn (Database connection): Connection redshift database.
    
    table (list): A list of strings that identify tables for validation. 
    
    Returns:
    None
    
    """
    log.info('Implementing DataQualityOperator now.')
    hook_redshift = PostgresHook(redshift_conn)
    for table in tables:
        records = hook_redshift.get_records(f"SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            log.error(f"Table: {table} returned nothing.")
        num_records = records[0][0]
        if num_records == 0:
            log.error(f"{table} returned no records in destination.")
        log.info(f"Data validation check on {table} is completed.")

################### CREATE TASKS FOR DAG #############################################
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

load_individual_confidence_data = PythonOperator(
    task_id='Combine_and_load_individual_confidence_data_to_Redshift',
    dag=dag,
    provide_context=True,
    python_callable=loadQuandl_yaleConf,
    op_kwargs = {'conn' : conn , 'redshift_table': 'us_stock_market_confidence_indices'}

)

load_yale_sp_Comp = PythonOperator(
    task_id='Load_SPComp_Yale_Data_to_Redshift',
    dag=dag,
    provide_context=True,
    python_callable=loadQuandl_yaleComp,
    op_kwargs = {'conn' : conn , 'redshift_table': 'sp_500_composite_hist'}
)

load_inv_sentiment = PythonOperator(
    task_id='Load_inv_sentiment_to_Redshift',
    dag=dag,
    provide_context=True,
    python_callable=loadQuandl_sentiment,
    op_kwargs = {'conn' : conn , 'redshift_table': 'invest_sentiment'}
)

load_ticker_history = PythonOperator(
    task_id='Load_ticker_history_to_redshift',
    dag=dag,
    provide_context=True,
    python_callable=get_ticker_data
    op_kwargs= {'conn' : conn, 'table': 'hist_market_data', 'wt_api_key': wt_key }
)

data_quality_check = PythonOperator(
    task_id='data_quality_check',
    dag=dag,
    provide_context=True,
    python_callable=data_quality_check,
    op_kwargs = {'redshift_conn' : conn, "tables" : rs_tables}
)


################### SPECIFY ORDER OF TASKS IN THE DAG ##################################
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> load_individual_confidence_data 
start_operator >> load_yale_sp_Comp 
start_operator >> load_inv_sentiment 

load_inv_sentiment >> load_ticker_history
load_yale_sp_Comp >> load_ticker_history
load_individual_confidence_data >> load_ticker_history

load_ticker_history >> data_quality_check

data_quality_check >> end_operator