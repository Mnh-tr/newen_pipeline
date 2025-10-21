import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
AIRFLOW_STMP_TO_ADDRESS = "minhtri0428@gmail.com"

default_args = {
    "owner": "airflow",
    # "start_date": datetime.datetime(2025, 10, 17, 7, 0, tzinfo=local_tz),
    "depends_on_past": False,
    "start_date": datetime.datetime(2025, 10, 14, tzinfo=local_tz), # testing
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=3),
    "email": AIRFLOW_STMP_TO_ADDRESS,
    "email_on_failure": True,
    "email_on_retry": True
    }

with DAG(
    "scraper_tiktok_web",
    default_args=default_args,
    schedule="0 7 * * *",
    catchup=False,
) as dag:
    start=EmptyOperator(task_id="start")

    get_config_for_scraper=BashOperator(
        task_id="get_config_for_scraper",
        bash_command="cd /opt/airflow/app && python src/extract/get_config.py"
    )

    scraper_tiktokweb_post=BashOperator(
        task_id="scraper_tiktokweb_post",
        bash_command="cd /opt/airflow/app && python src/extract/scraper_tiktokweb_post.py"
    )

    transform_tiktokweb_post=BashOperator(
        task_id="transform_tiktokweb_post",
        bash_command="cd /opt/airflow/app && python src/transform/transform_tiktokpost.py"
    )

    upload_tiktokweb_post=BashOperator(
        task_id="upload_tiktokweb_post",
        bash_command="cd /opt/airflow/app && python src/upload/upload_tiktokpost.py"
    )

    end=EmptyOperator(task_id="end")
    start >> get_config_for_scraper >> scraper_tiktokweb_post >> transform_tiktokweb_post >> upload_tiktokweb_post >> end