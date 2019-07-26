from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "dub"
    operators = [
        operators.DataQualityOperator,
        operators.LoadQuandlOperator
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.Quandl_Conf
    ]
