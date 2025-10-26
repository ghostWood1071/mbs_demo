from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig

PROJECT_DIR = "/opt/airflow/dags/mbs_demo"

PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.2.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.postgresql:postgresql:42.7.3",
    "org.apache.hive:hive-metastore:3.1.3",
    "org.apache.hive:hive-exec:3.1.3"
])

DBT_PROFILE_DICT = {
    "spark_profile": {
        "target": "prod",
        "outputs": {
            "prod": {
                "type": "spark",
                "method": "session",
                "schema": "default",
                "threads": 4,
                "spark_conf": {
                    # Kubernetes (driver là task pod)
                    "spark.master": "k8s://https://kubernetes.default.svc",
                    "spark.submit.deployMode": "cluster",
                    "spark.kubernetes.namespace": "compute",
                    "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",

                    # Ảnh executors (nên đồng nhất lib với driver)
                    "spark.kubernetes.container.image": "ghostwood/spark-delta-samp-job:latest",
                    # Tài nguyên executors
                    "spark.executor.instances": "2",
                    "spark.executor.cores": "1",
                    "spark.executor.memory": "2g",

                    #Catalog
                    "spark.sql.warehouse.dir": "s3a://warehouse",
                    "spark.sql.catalogImplementation": "hive",

                    # Delta
                    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

                    # MinIO S3A
                    "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000",
                    "spark.hadoop.fs.s3a.path.style.access": "true",
                    # Nếu không dùng secret mà set thẳng:
                    "spark.hadoop.fs.s3a.access.key": "minioadmin",
                    "spark.hadoop.fs.s3a.secret.key": "minio@demo!",

                    # (khuyến nghị) ổn định
                    "spark.network.timeout": "600s",
                    "spark.executor.heartbeatInterval": "60s",
                    "spark.jars.packages": PACKAGES,
                    "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp"
                }
            }
        }
    }
}

project_cfg = ProjectConfig(dbt_project_path=PROJECT_DIR)
profile_cfg = ProfileConfig.from_dict("spark_profile", "prod", DBT_PROFILE_DICT)
exec_cfg = ExecutionConfig(dbt_executable_path="dbt")

customer_dbt = DbtDag(
    dag_id="customer_dbt_cosmos_session",
    start_date=datetime(2025, 10, 25),
    schedule_interval="@daily",
    catchup=False,
    project_config=project_cfg,
    profile_config=profile_cfg,
    execution_config=exec_cfg,
)
