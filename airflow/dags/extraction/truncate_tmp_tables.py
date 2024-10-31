import psycopg2 as psy
from urllib.parse import quote
from sqlalchemy import create_engine, text
import warnings
from config import *




username = quote(sql_username)
password = sql_password

host = sql_host
port = sql_port
database = sql_database

connection_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
engine = create_engine(connection_string)



with engine.connect() as conn:
    conn.execution_options(isolation_level="AUTOCOMMIT")
    conn.execute(text("truncate table epl_league_table_tmp"))
    conn.execute(text("truncate table epl_league_table_tmp_no_xg"))