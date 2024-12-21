# EPL_Table_Viz
## Visualizing the EPL (English Premier League) table for every season.

## Data Extraction
The project utilizes Pandas Python library to scrape the league position of each PL season data from [FbRef](https://fbref.com/) from 1888 till the current season. There are 2 sets of dataframes to consider, i.e., one with information on xG, xGA and one without (xG tracking only began from 2015 from the datasource). 

## Data Preprocessing
Supabase (Postgres) is the database which consists of 2 temp table in which the python script dumps the data into their specific temp tables and a merge operation concatenates the data into a primary table which becomes the source of truth for our usecase.
![Supabase Table Data After Merge](/Images/Supabase_img.png)


## Data Orchestration
![Airflow page](/Images/Airflow_Img.png)
As the EPL is played every week (excluding summer break and the congested winter season), the data needs to be updated at the minimum every week. Airflow is used to run the scripts in a Docker container. The temp table are truncated first, then only the latest data is received from the current ongoing season. The latest season is extracted from the primary table and the loop to extract data from seasons that do not exist or have not been updated are dumped into the temp table. After retrieving the data into the temp table, it is merged into the primary table using specific joins and conditions so as to not overwrite the data that already exists. The data is updated only when there are changes in the latest season data. The SQL and Python scripts run in a sequential manner to perform incremental load so as to not overload the dump size into the temp tables.

![Airflow Successful Run](/Images/Airflow_Img_Success.png)

## Data Visualization
Streamlit is a Python framework used to display the matplotlib visualization of the table with a dropdown for selecting which season the user wants to look at.

![Screenshot of the webpage displaying the table](/Images/webpage.png)


Using Supabase(Postgres), Python, Pandas, Matplotlib, Airflow, Docker and Streamlit, the project pipeline can be hosted on a cloud server to update the database for each season without any modifications required.
