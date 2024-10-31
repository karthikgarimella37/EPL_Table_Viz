from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

'''
DAG to extract EPL table data from Fbref into supabase Postgresql DB
'''

output_name = datetime.now().strftime("%Y%m%d")

schedule_interval = "@once"
start_date = days_ago(1)


default_args = {"owner": "airflow", "depends_on_past": False, "retries": 0}
epl_data = "EPL_Data.py"
truncate_tmp_tables = "truncate_tmp_tables.py"
merge_main = "merge_tmp_to_main.py"



with DAG(
    dag_id = "etl_epl_pipeline",
    description = "EPL Data Pipeline",
    schedule_interval = schedule_interval,
    default_args = default_args,
    start_date = start_date,
    catchup = True,
    max_active_runs = 1,
    tags = ["EPL_ETL"],
) as dag:

    extract_epl_data = BashOperator(
        task_id = "extract_epl_data",
        bash_command = f"python /opt/airflow/dags/extraction/EPL_Data.py {output_name}",
        dag = dag,
    )
    extract_epl_data.doc_md = "Extract EPL data and dump into tmp table"

    truncate_tmp = BashOperator(
        task_id = "truncate_tmp",
        bash_command = f"python /opt/airflow/dags/extraction/truncate_tmp_tables.py {output_name}",
        dag = dag,
    )
    truncate_tmp.doc_md = "Truncate tmp tables before data dump of latest data"

    merge_to_main_table = BashOperator(
        task_id = "merge_to_main_table",
        bash_command = f"python /opt/airflow/dags/extraction/merge_tmp_to_main.py {output_name}",
        dag = dag,
    )
    merge_to_main_table.doc_md = "Merge to main EPL table from tmp table"


truncate_tmp >> extract_epl_data >> merge_to_main_table

