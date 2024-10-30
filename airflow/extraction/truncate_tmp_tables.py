import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt 
import psycopg2 as psy
import re
import requests
from bs4 import BeautifulSoup
import seaborn as sns
import os
import datetime
import json
from urllib.request import urlopen
from urllib.parse import quote
# from PIL import Image
# from highlight_text import fig_text
# from mplsoccer import Bumpy, FontManager, add_image
from sqlalchemy import create_engine, text
from requests_ip_rotator import ApiGateway, EXTRA_REGIONS
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