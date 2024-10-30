import streamlit as st
import numpy as np
import pandas as pd
import requests
import matplotlib
import matplotlib.pyplot as plt
import psycopg2 as psy
import datetime
import json
from urllib.request import urlopen
from sqlalchemy import create_engine, text
from urllib.parse import quote
from requests_ip_rotator import ApiGateway, EXTRA_REGIONS
import decimal
import io
from plottable import ColumnDefinition, Table
from plottable.cmap import normed_cmap
from plottable.plots import image

# engine = create_engine('postgresql+psycopg2://postgres:Karthik37@localhost:5432/Football')

# conn = engine.connect()

username = quote(st.secrets["connections"]["postgresql"]["username"])
password = st.secrets["connections"]["postgresql"]["password"]

host = st.secrets["connections"]["postgresql"]['host']
port = st.secrets["connections"]["postgresql"]['port']
database = st.secrets["connections"]["postgresql"]['database']

connection_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'

engine = create_engine(connection_string)
conn = engine.connect()
# conn = st.connection("postegresql", type = 'sql')

df = conn.execute(text('select * from epl_league_table'))
df = pd.DataFrame(df)

# df = pd.read_csv('db_data.csv', index_col=False)
# df = df.drop(columns = ['Unnamed: 0'])

df[['xg', 'xga', 'xgd', 'xgd_per_90']] = df[['xg', 'xga', 'xgd', 'xgd_per_90']].astype(float)

# st.dataframe(df.style.highlight_max(axis=0))

seasons = df['season'].sort_values(ascending = False).unique()
selected_season = st.selectbox("Select Season", seasons)

df = df[df['season'] == selected_season]


df = df[[
    'league_position', 'club_name', 'matches_played', 'wins',
       'draws', 'losses', 'goals_scored', 'goals_conceded', 'goal_difference',
       'points', 'points_per_match', 'xg', 'xga', 'xgd', 'xgd_per_90',
    #    'avg_attendance', 'top_team_scorer', 'goalkeeper', 'notes',
        'season'
]]
column_mapping = {
    "league_position": "Rank",
    "club_name": "Club",
    "matches_played": "MP",
    "wins": "W",
    "draws": "D",
    "losses": "L",
    "goals_scored": "G",
    "goals_conceded": "GA",
    "goal_difference": "GD",
    "points": "P",
    "points_per_match": "P/90",
    "xg": "xG",
    "xga": "xGA",
    "xgd": "xGD",
    "xgd_per_90": "xGD/90",
    # "avg_attendance": "Avg Attendance",
    # "top_team_scorer": "Top Scorer",
    # "goalkeeper": "GK",
    # "notes": "Notes",
    "season": "Season"
}

df.rename(columns=column_mapping, inplace=True)
df = df.sort_values(by = ['Season', 'Rank']).reset_index(drop = True)

bg_color = "#FFFFFF"
text_color = "#000000" 

row_colors = {
    "top4": "#E1FABC",
    "top6": "#FFFC97",
    "relegation": "#E79A9A",
    "even": "#E2E2E1",
    "odd": "#B3B0B0",
}

plt.rcParams["text.color"] = text_color
plt.rcParams["font.family"] = "monospace"
col_defs = [
    ColumnDefinition(
        name="Rank",
        textprops={"ha": "center"},
        width=0.5,
    ),
    ColumnDefinition(
        name="Club",
        textprops={"ha": "left", "weight": "bold"},
        width=1.75,
    ),
    ColumnDefinition(
        name="MP",
        group="Matches Played",
        textprops={"ha": "center"},
        width=0.5,
    ),
    ColumnDefinition(
        name="W",
        group="Matches Played",
        textprops={"ha": "center"},
        width=0.5,
    ),
    ColumnDefinition(
        name="D",
        group="Matches Played",
        textprops={"ha": "center"},
        width=0.5,
    ),
    ColumnDefinition(
        name="L",
        group="Matches Played",
        textprops={"ha": "center"},
        width=0.5,
    ),
    ColumnDefinition(
        name="G",
        group="Goals",
        textprops={"ha": "center"},
        width=0.5,
    ),
    ColumnDefinition(
        name="GA",
        group="Goals",
        textprops={"ha": "center"},
        width=0.5,
    ),
    ColumnDefinition(
        name="GD",
        group="Goals",
        textprops={"ha": "center"},
        width=0.5,
    ),
    ColumnDefinition(
        name="P",
        group="Points",
        textprops={"ha": "center"},
        width=0.5,
    ),
    ColumnDefinition(
        name="P/90",
        group="Points",
        textprops={"ha": "center"},
        width=0.5,
    ),
    ColumnDefinition(
        name="xG",
        group="Expected Goals",
        textprops={"ha": "center", "color": "#000000", "weight": "bold", "bbox": {"boxstyle": "circle", "pad": 0.35}},
        cmap=normed_cmap(df["xG"], cmap=matplotlib.cm.PiYG, num_stds=2)
    ),
    ColumnDefinition(
        name="xGA",
        group="Expected Goals",
        textprops={"ha": "center", "color": "#000000", "weight": "bold", "bbox": {"boxstyle": "circle", "pad": 0.35}},
        cmap=normed_cmap(df["xGA"], cmap=matplotlib.cm.PiYG_r, num_stds=2)
    ),
    ColumnDefinition(
        name="xGD",
        group="Expected Goals",
        textprops={"ha": "center", "color": "#000000", "weight": "bold", "bbox": {"boxstyle": "circle", "pad": 0.35}},
        cmap=normed_cmap(df["xGD"], cmap=matplotlib.cm.PiYG, num_stds=2)
    ),
    ColumnDefinition(
        name="xGD/90",
        group="Expected Goals",
        textprops={"ha": "center", "color": "#000000", "weight": "bold", "bbox": {"boxstyle": "circle", "pad": 0.35}},
        cmap=normed_cmap(df["xGD/90"], cmap=matplotlib.cm.PiYG, num_stds=2)
    ),
]
fig, ax = plt.subplots(figsize=(22, 30))
fig.set_facecolor(bg_color)
ax.set_facecolor(bg_color)
table = Table(
    df,
    column_definitions=col_defs,
    index_col="Rank",
    row_dividers=True,
    row_divider_kw={"linewidth": 1, "linestyle": (0, (1, 5))},
    footer_divider=True,
    textprops={"fontsize": 18},
    col_label_divider_kw={"linewidth": 1, "linestyle": "-"},
    column_border_kw={"linewidth": .5, "linestyle": "-"},
    ax=ax,
).autoset_fontcolors(colnames=["xG", "xGA", "xGD", "xGD/90"]) # This will set the font color of the columns based on the cmap so the text is readable

table.cells[10, 3].textprops["color"] = "#8ACB88"

# fig.tight_layout()


for idx in [0, 1, 2, 3]:
    table.rows[idx].set_facecolor(row_colors["top4"])

for idx in [4, 5]:
    table.rows[idx].set_facecolor(row_colors["top6"])

# if int(df.iloc[0]['Season'][:4]) > 1905:
for idx in df.iloc[df.shape[0] - 3:].index:
    table.rows[idx].set_facecolor(row_colors["relegation"])


st.pyplot(fig, use_container_width = True)
