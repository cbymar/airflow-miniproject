import yfinance as yf
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "chris",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 7),
    "email": ["foo@bar.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "pp_stock",
    default_args=default_args,
    user_defined_macros={"env": Variable.get("environment")},
    schedule_interval="5 * * * *"
)

tempdir_templated="""
    mkdir -p /tmp/data/{{ ds_nodash }}
"""

put_hdfs_templated="""
    hdfs_put.sh {{ ds_nodash }} {{ params.symbol }}
"""



def pull_from_yf(symbol: str):
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    stock_df = yf.download(symbol, start=start_date, end=end_date, interval='1m')
    stock_df.to_csv(f"/tmp/data/{start_date}/stk_{symbol}_df.csv", header=False)

def query_hdfs():
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    stock_df = yf.download(symbol, start=start_date, end=end_date, interval='1m')
    stock_df.to_csv(f"/tmp/data/{start_date}/stk_{symbol}_df.csv", header=False)


# with dag as dag:
make_temp = BashOperator(
    task_id="make_csv_tempdir",
    bash_command=tempdir_templated,
    dag=dag)

t1 = PythonOperator(
    task_id="pull_AAPL",
    python_callable=pull_from_yf,
    op_kwargs={"symbol": "AAPL"},
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id="pull_TSLA",
    python_callable=pull_from_yf,
    op_kwargs={"symbol": "TSLA"},
    provide_context=True,
    dag=dag,
)

t3 = BashOperator(
  task_id="csv_to_hdfs_AAPL",
  bash_command=put_hdfs_templated,
  params={"symbol": "AAPL"},
  dag=dag)

t4 = BashOperator(
    task_id="csv_to_hdfs_TSLA",
    bash_command=put_hdfs_templated,
    params={"symbol": "TSLA"},
    dag=dag)

t5 = PythonOperator(
    task_id="custom_query",
    python_callable=query_hdfs,
    provide_context=True,
    dag=dag,
)


make_temp >> [t1, t2]
t1 >> t3
t2 >> t4
[t3, t4] >> t5
