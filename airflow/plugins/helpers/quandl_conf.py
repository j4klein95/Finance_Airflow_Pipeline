import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import json
import quandl as qdl

class Quandl_Conf:
    def confidence_data():
    
    # Make API call to get Confidence Tables:
        raw_valuation_conf = qdl.get("YALE/US_CONF_INDEX_VAL_INDIV", paginate=True).reset_index()
        raw_crash_conf = qdl.get("YALE/US_CONF_INDEX_CRASH_INDIV", paginate=True).reset_index()
        raw_buy_dips = qdl.get("YALE/US_CONF_INDEX_BUY_INDIV", paginate=True).reset_index()
    
    # Rename the Columns of Each Table:
        val_conf = raw_valuation_conf.rename(columns={"Index Value": "Index_Value_Valuation_Confidence", "Standard Error": "Standard_Error_Valuation"})
        crash_conf = raw_crash_conf.rename(columns={"Index Value": "Index_Value_Crash_Confidence", "Standard Error": "Standard_Error_Crash"})
        bod_conf = raw_buy_dips.rename(columns={"Index Value": "Index_Value_Buy_on_Dip_Confidence", "Standard Error": "Standard_Error_Buy_on_Dip"})
    
    #Merge Tables:
        confidence_ = pd.merge(val_conf, crash_conf, on="Date")
        confidence_final = pd.merge(confidence_, bod_conf, on="Date")
    
    #return final table:
        return confidence_final