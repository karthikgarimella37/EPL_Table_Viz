import numpy as np 
import pandas as pd 
import psycopg2 as psy
import re
import requests
import os
import datetime
import json
from urllib.request import urlopen
from urllib.parse import quote
from sqlalchemy import create_engine, text
from requests_ip_rotator import ApiGateway, EXTRA_REGIONS
import warnings
from config import *

warnings.filterwarnings('ignore')

os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
os.environ['AWS_DEFAULT_REGION'] = aws_default_region

# Initialize the ApiGateway
gateway = ApiGateway('https://fbref.com/', regions = EXTRA_REGIONS)
gateway.start()

# Start session
session = requests.Session()
session.mount('https://fbref.com/', gateway)

df = pd.DataFrame()


username = quote(sql_username)
password = sql_password

host = sql_host
port = sql_port
database = sql_database

connection_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
engine = create_engine(connection_string)



with engine.connect() as conn:
    latest_season_sql = conn.execute(text('select max(season) from epl_league_table'))
    latest_season = latest_season_sql.fetchone()[0][0:4]
    


# Looping for a set range - incremental load will be done from 2024-25 season
for i in range(int(latest_season), datetime.datetime.now().year + 1):
    print("Entered Loop")
    try:
        # Getting the site for each consecutive year
        
        fbref_site_url = f'https://fbref.com/en/comps/9/{i}-{i+1}/{i}-{i+1}-Premier-League-Stats#all_stats_squads_standard'
        response = session.get(fbref_site_url)
        fbref_site = response.text

        # Using pandas read_html to get the table
        fbref_table = pd.read_html(fbref_site)[0]
        fbref_table['season'] = str(i) + '/' + str(i+1)

        if 'Last 5' in fbref_table:
            fbref_table = fbref_table.drop(columns = ['Last 5'])

        if fbref_table.iloc[0]['MP'] < 38 and (fbref_table.iloc[0]['MP'] < 38 and fbref_table.iloc[0]['season'] != f'{int(latest_season)}/{int(latest_season) + 1}'):
            print(int(latest_season), int(latest_season) + 1)
            continue

        else:
            df = pd.concat([df, fbref_table])
        

            print(df)
            print('Data Extracted')

            
            # input()
            if "xG" in fbref_table.columns:
                column_names = ["league_position", "club_name", "matches_played", "wins", "draws", "losses",
                            "goals_scored", "goals_conceded", "goal_difference", "points", "points_per_match",
                            "xg", "xga", "xgd", "xgd_per_90", "avg_attendance",
                            "top_team_scorer", "goalkeeper", "notes", 'season']
            else:
                column_names = ["league_position", "club_name", "matches_played", "wins", "draws", "losses",
                            "goals_scored", "goals_conceded", "goal_difference", "points", "points_per_match",
                            "avg_attendance", "top_team_scorer", "goalkeeper", "notes", 'season']
            print('Columns created')

                # Renaming columns in the DataFrame
            fbref_table = fbref_table.rename(columns=dict(zip(fbref_table.columns, column_names)))
            
            print('Table with new columns created')
            # Creating engine to connect to PostgreSQL DB

            print(fbref_table)




            print('Engine Created')


            # Inserting into the temp table

            with engine.connect() as conn:
                # table_name = 'epl_league_table_tmp' if 'xg' in fbref_table.columns else 'epl_league_table_tmp_no_xg'
                # fbref_table.to_sql(table_name, conn, if_exists='append', index=False)
                # print(f"Data for season {i}/{i + 1} appended to {table_name}.")

                if "xg" in fbref_table.columns:
                    fbref_table.to_sql('epl_league_table_tmp', conn, if_exists = 'append', index = False)
                    print('Data Appened to the tmp table')
                else:
                    fbref_table.to_sql('epl_league_table_tmp_no_xg', conn, if_exists = 'append', index = False)
                    print('Data Appened to the tmp table')

            conn.close
    except Exception as e:
        print('Halted, Error')
        print(e)
        conn.close()
        continue


print('Data Dump Done!')

# To generate csv file of the database table
# df.to_csv('db_data.csv')


