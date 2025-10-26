from datetime import datetime
from airflow import DAG
from cosmos import DbtDag
from cosmos.config import ProjectConfig, ProfileConfig, ExecutionConfig


PROJECT_DIR = "/opt/airflow/dags/mbs_demo/dbt/customer_dbt"
PROFILES_YML = "/opt/airflow/dags/mbs_demo/profiles/profiles.yml"

# === Cosmos configs (API đúng) ===
project_cfg = ProjectConfig(dbt_project_path=PROJECT_DIR)

profile_cfg = ProfileConfig(
    profile_name="spark_profile",
    target_name="prod",
    profiles_yml_filepath=PROFILES_YML,
)

exec_cfg = ExecutionConfig(
    dbt_executable_path="dbt"
)

# === Airflow DAG ===
with DAG(
    dag_id="customer_dbt_cosmos_session",
    description="Run dbt (spark, method=session) on K8s using Hive catalog + MinIO",
    start_date=datetime(2025, 10, 26),
    catchup=False,
    tags=["dbt", "spark", "cosmos", "delta", "hive"],
) as dag:

    # Cách gọn nhất: để Cosmos tự render chuỗi lệnh dbt run/test toàn project.
    dbt_pipeline = DbtDag(
        dag_id="customer_dbt_cosmos_session_inner",  # id nội bộ cho TaskGroup/DAG con
        project_config=project_cfg,
        profile_config=profile_cfg,
        execution_config=exec_cfg,
    )
