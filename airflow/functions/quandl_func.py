from sqlalchemy import create_engine
import pandas as pd
from pandas.io.json import json_normalize
import quandl as qdl
import logging

def loadQuandl_sentiment(conn, redshift_table):
    log.info('Beginning implementation of Quandl-Redshift Load.)
    
    raw_sentiments = qdl.get("AAII/AAII_SENTIMENT", paginate=True)
    filtered_sentiments = raw_sentiments.iloc[:, :-3]
    finished_sentiments = filtered_sentiments.reset_index()
    
    finished_sentiments.to_sql(table, conn, index=False, if_exists='replace')
    log.info(f'Successfully loaded Investor Sentiment data to {table}.')
    
    

def loadQuandl_yaleComp(conn, redshift_table):
    log.info('Beginning implementation of Quandl-Redshift Load for Yale S&P Data.')
             
    raw_sp_comp = qdl.get("YALE/SPCOMP", paginate=True)
    finished_data = raw_sp_comp.reset_index()
             
    finished_data.to_sql(table, conn, index=False, if_exists='replace')
    log.info(f'Successfully loaded Yale S&P Composite data to {table}')
             
    
def loadQuandl_yaleConf(conn, redshift_table):
    log.info('Beginning implementation of Quandl-Redshift Load for Yale S&P Data.')

    raw_valuation_conf = qdl.get("YALE/US_CONF_INDEX_VAL_INDIV", paginate=True).reset_index()
    raw_crash_conf = qdl.get("YALE/US_CONF_INDEX_CRASH_INDIV", paginate=True).reset_index()
    raw_buy_dips = qdl.get("YALE/US_CONF_INDEX_BUY_INDIV", paginate=True).reset_index()
             
    val_conf = raw_valuation_conf.rename(columns={"Index Value": "Index_Value_Valuation_Confidence", "Standard Error": "Standard_Error_Valuation"})
    crash_conf = raw_crash_conf.rename(columns={"Index Value": "Index_Value_Crash_Confidence", "Standard Error": "Standard_Error_Crash"})
    bod_conf = raw_buy_dips.rename(columns={"Index Value": "Index_Value_Buy_on_Dip_Confidence", "Standard Error": "Standard_Error_Buy_on_Dip"})

    confidence_ = pd.merge(val_conf, crash_conf, on="Date")
    confidence_final = pd.merge(confidence_, bod_conf, on="Date")
             
    confidence_final.to_sql(table, conn, index=False, if_exists='replace')