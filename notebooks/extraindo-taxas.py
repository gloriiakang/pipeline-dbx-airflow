from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    'Executando-notebook-etl',
    start_date=datetime(2025, 7, 26),
    schedule_interval="0 9 * * *",
    ) as dag_executando_notebook_extracao:

        extract_data = DatabricksRunNowOperator(
        task_id = 'Extraindo-conversoes',
        databricks_conn_id = 'databricks_default',
        job_id = 147207196401709,
        notebook_params={"execution_date": '{{data_interval_end.strftime("%Y-%m-%d")}}'}
        )

        transforming_data = DatabricksRunNowOperator(
        task_id = 'transformando-dados',
        databricks_conn_id = 'databricks_default',
        job_id = 378198228476432
        )

        sending_report = DatabricksRunNowOperator(
        task_id = 'enviando-relatorio',
        databricks_conn_id = 'databricks_default',
        job_id = 168265679940527
        )


        extract_data >> transforming_data >> sending_report