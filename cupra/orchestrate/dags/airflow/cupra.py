import datetime
from copy import deepcopy
from datetime import timedelta

import pendulum
from airflow import models
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from comparison_package import ComparisonTrigger
from kubernetes.client import models as k8s_models
from airflow.operators.python import PythonOperator


IMAGE = "australia-southeast1-docker.pkg.dev/cupra-main/meltano/meltano-cupra-main:prod"
PROJECT_NAME = "cupra-main"

local_tz = pendulum.timezone("Pacific/Auckland")
comparison_start_date = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "retry_delay": timedelta(minutes=30),
    "start_date": datetime.datetime(2026, 5, 19, tzinfo=local_tz),
}


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_cupra_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main",deserialize_json=True)
    meltano_env_ga4 = Variable.get("meltano_developer_ga4_main",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique, **meltano_env_ga4}
    start_date_str = (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
    ).strftime("%Y-%m-%d")
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def set_env_vars_facebook():
    env = get_meltano_env()
    env["BQ_DATASET"] = "facebook_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "facebook_transformed"
    return env


def set_env_vars_cm360():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "cm360_transformed"
    return env


def set_env_vars_dv360():
    env = get_meltano_env()
    env["BQ_DATASET"] = "dv360_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "dv360_transformed"
    return env

def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "dash_table"
        return env


with models.DAG(
    dag_id="cupra-meltano-extraction-transformation-dbt",
    schedule_interval="0 14 * * *",
    default_args=default_args,
) as dag:
    kube_facebook = KubernetesPodOperator(
        name="cupra-facebook-to-bigquery",
        task_id="cupra-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-facebook",
            "target-bigquery",
            "dbt-bigquery:facebook_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_facebook(),
        get_logs=True,
    )

    kube_cm360 = KubernetesPodOperator(
        name="cupra-cm360-to-bigquery",
        task_id="cupra-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360(),
        get_logs=True,
    )

    kube_dv360 = KubernetesPodOperator(
        name="cupra-dv360-to-bigquery",
        task_id="cupra-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-dv360",
            "target-bigquery",
            "dbt-bigquery:dv360_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360(),
        get_logs=True,
    )
    kube_dash = KubernetesPodOperator(
        name="cupra-dash-to-bigquery",
        task_id="cupra-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule="all_done",
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_table",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        get_logs=True,
    )
    env = get_meltano_env()
    comparison_trigger_facebook = ComparisonTrigger(
        project_name="cupra-main",
        destination_table="facebook_transformed",
        table_name="facebook",
        source_name="meta",
        start_date=comparison_start_date,
        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
        secret_name="airflow-variables-meltano_cupra_main",
        project_id=env["PROJECT_ID"]
    )
    def facebook_comparison_check(**context):
        result = comparison_trigger_facebook.compare_data()
        if not result:
            raise ValueError("Facebook data accuracy check failed — BQ data does not match source API.")
        return result
    task_facebook_comparison = PythonOperator(
        task_id="task_facebook_comparison",
        python_callable=facebook_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    kube_facebook >> task_facebook_comparison
    comparison_trigger_dv360_standard = ComparisonTrigger(
        project_name="cupra-main",
        destination_table="dv360_transformed",
        table_name="dv360_standard",
        source_name="dv360_standard",
        start_date=comparison_start_date,
        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
        secret_name="airflow-variables-meltano_cupra_main",
        project_id=env["PROJECT_ID"]
    )
    def dv360_standard_comparison_check(**context):
        result = comparison_trigger_dv360_standard.compare_data()
        if not result:
            raise ValueError("DV360 data accuracy check failed — BQ data does not match source API.")
        return result
    task_dv360_standard_comparison = PythonOperator(
        task_id="task_dv360_standard_comparison",
        python_callable=dv360_standard_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    comparison_trigger_dv360_youtube = ComparisonTrigger(
        project_name="cupra-main",
        destination_table="dv360_transformed",
        table_name="dv360_youtube",
        source_name="dv360_youtube",
        start_date=comparison_start_date,
        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d")-timedelta(days=1),
        secret_name="airflow-variables-meltano_cupra_main",
        project_id=env["PROJECT_ID"]
    )
    def dv360_youtube_comparison_check(**context):
        result = comparison_trigger_dv360_youtube.compare_data()
        if not result:
            raise ValueError("DV360 data accuracy check failed — BQ data does not match source API.")
        return result
    task_dv360_youtube_comparison = PythonOperator(
        task_id="task_dv360_youtube_comparison",
        python_callable=dv360_youtube_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    kube_dv360 >> [task_dv360_standard_comparison , task_dv360_youtube_comparison]
    kube_dash_union = KubernetesPodOperator(
        name="cupra-dash-union-to-bigquery",
        task_id="cupra-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_union",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        get_logs=True,
    )




    kube_cm360 >> kube_dv360
    [kube_dv360,kube_facebook] >> kube_dash
    kube_dash >> kube_dash_union


with models.DAG(
    dag_id="cupra-meltano-ga4",
    schedule_interval="30 15 * * *",
    default_args=default_args,
) as ga4_dag:

    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "dash_table"
        return env

    def set_env_vars_dash_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "dash_table_search"
        return env

    def set_env_vars_ga4(goal):
        env = get_meltano_env()
        if goal == "session":
            env["TAP_GA4_REPORTS"] = "./report_sessions.json"
            env["GA4_GOAL"] = "session_goal"
        else:
            env["TAP_GA4_REPORTS"] = "./report.json"
            env["GA4_GOAL"] = "goal"
        env["BQ_DATASET"] = "ga4_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_DATASET"] = "ga4_transformed"
        developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
        developer_creds.refresh(Request())
        env["TAP_GA4_START_DATE"] = get_ga4_start_date()
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        return env

    def set_env_vars_ga4_final():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "ga4_transformed"
        return env



    kube_dash = KubernetesPodOperator(
        name="cupra-dash-to-bigquery",
        task_id="cupra-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule="all_done",
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_table",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        get_logs=True,
    )

    kube_dash_search = KubernetesPodOperator(
        name="cupra-dash-search-to-bigquery",
        task_id="cupra-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_table_search",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search(),
        get_logs=True,
    )

    kube_dash_union = KubernetesPodOperator(
        name="cupra-dash-union-to-bigquery",
        task_id="cupra-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_union",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        get_logs=True,
    )

    kube_ga4_final = KubernetesPodOperator(
        name="cupra-ga4-final-to-bigquery",
        task_id="cupra-ga4_final_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:ga4_final_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4_final(),
        get_logs=True,
    )

    kube_ga4_list = []
    for goal in ("goal", "session"):
        kube_ga4 = KubernetesPodOperator(
            name=f"cupra-ga4-{goal}-to-bigquery",
            task_id=f"cupra-ga4_{goal}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "run",
                "tap-ga4",
                "target-bigquery",
                f"dbt-bigquery:ga4_{goal}_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(goal),
            get_logs=True,
        )
        kube_ga4_list.append(kube_ga4)

    kube_dash >> kube_dash_search >> kube_dash_union
    for kube_ga4 in kube_ga4_list:
        kube_dash_union >> kube_ga4 >> kube_ga4_final
