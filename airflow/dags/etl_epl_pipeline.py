from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

'''
DAG to extract EPL table data from Fbref into supabase Postgresql DB
'''

