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

merge_query = """merge into epl_league_table epl
using
(select * from epl_league_table_tmp tmp) tmp
on epl.club_name = tmp.club_name
and epl.season = tmp.season
when not matched
then insert
(league_position,
club_name,
matches_played,
wins,
draws,
losses,
goals_scored,
goals_conceded,
goal_difference,
points,
points_per_match,
xg,
xga,
xgd,
xgd_per_90,
avg_attendance,
top_team_scorer,
goalkeeper,
notes,
season,
country,
continent,
league_tier,
gender,
datasource)
values(
tmp.league_position,
tmp.club_name,
tmp.matches_played,
tmp.wins,
tmp.draws,
tmp.losses,
tmp.goals_scored,
tmp.goals_conceded,
tmp.goal_difference,
tmp.points,
tmp.points_per_match,
tmp.xg,
tmp.xga,
tmp.xgd,
tmp.xgd_per_90,
tmp.avg_attendance,
tmp.top_team_scorer,
tmp.goalkeeper,
tmp.notes,
tmp.season,
'England',
'Europe',
'1',
'Male',
'Fbref')
when matched
then update
set
league_position = tmp.league_position,
club_name = tmp.club_name,
matches_played = tmp.matches_played,
wins = tmp.wins,
draws = tmp.draws,
losses = tmp.losses,
goals_scored = tmp.goals_scored,
goals_conceded = tmp.goals_conceded,
goal_difference = tmp.goal_difference,
points = tmp.points,
points_per_match = tmp.points_per_match,
xg = tmp.xg,
xga = tmp.xga,
xgd = tmp.xgd,
xgd_per_90 = tmp.xgd_per_90,
avg_attendance = tmp.avg_attendance,
top_team_scorer = tmp.top_team_scorer,
goalkeeper = tmp.goalkeeper,
notes = tmp.notes,
season = tmp.season,
country = 'England',
continent = 'Europe',
league_tier = '1',
gender = 'Male',
datasource = 'Fbref'
"""

with engine.connect() as conn:
    conn.execution_options(isolation_level="AUTOCOMMIT")
    try:
        conn.execute(text(merge_query))
        print("Merge Completed Successfully")
    except Exception as e:
        print(f"Error Executing Merge Query: {e}")