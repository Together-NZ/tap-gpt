"""
Airflow DAG for wcet Meltano pipeline (Facebook + TikTok + DV360, Beervana).

See cupra/orchestrate/dags/airflow/cupra.py for full reference.
"""

import datetime
from copy import deepcopy
from datetime import timedelta
from google.oauth2.credentials import Credentials
import pendulum
from airflow import models
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s_models
from google.auth.transport.requests import Request

IMAGE = "australia-southeast1-docker.pkg.dev/wcet-main/meltano/meltano-wcet-main:prod"
PROJECT_NAME = "wcet-main"

local_tz = pendulum.timezone("Pacific/Auckland")

default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "retry_delay": timedelta(minutes=30),
    "start_date": datetime.datetime(2026, 5, 28, tzinfo=local_tz),
}
def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_wcet_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
    ).strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def set_env_vars_facebook(brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"facebook_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"facebook_transformed__{brand}"
    return env


def set_env_vars_tiktok(brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"tiktok_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"tiktok_transformed__{brand}"
    return env


def set_env_vars_dv360(brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"dv360_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"dv360_transformed__{brand}"
    return env

def set_env_vars_ga4(brand,goal):
    env = get_meltano_env()
    #if goal == 'ecommerce':
    if goal == 'sessions':
            env["TAP_GA4_REPORTS"] = "./report_sessions.json"
            env["GA4_GOAL"] = 'session_goal'
    else:
            env["TAP_GA4_REPORTS"] = "./report.json"
            env["GA4_GOAL"] = 'goal'   
    env["BQ_DATASET"] = f"ga4_raw__{brand}"
    env["BQ_METHOD"] = "gcs_stage"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_AUTH_METHOD"]='oauth'
    env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed__{brand}'       
    developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
    developer_creds.refresh(Request())
    env["TAP_GA4_START_DATE"]  = get_ga4_start_date()
    env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
    return env

def set_env_vars_google_ads(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"google_ads_search_transformed__{brand}"
    return env

def set_env_vars_dash(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"dash_table__{brand}"
    return env


with models.DAG(
    dag_id="wcet-meltano-extraction-transformation-dbt",
    schedule_interval="0 14 * * *",
    default_args=default_args,
    tags=["wcet", "meltano", "beervana"],
) as dag:
    brands = ['beervana']
    for brand in brands:
        kube_facebook = KubernetesPodOperator(
            name="wcet-facebook-to-bigquery",
            task_id="wcet-facebook_to_bigquery",
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
            env_vars=set_env_vars_facebook(brand),
            get_logs=True,
        )

        kube_tiktok = KubernetesPodOperator(
            name="wcet-tiktok-to-bigquery",
            task_id="wcet-tiktok_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "run",
                "tap-tiktok",
                "target-bigquery",
                "dbt-bigquery:tiktok_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_tiktok(brand),
            get_logs=True,
        )
        kube_dash = KubernetesPodOperator(
            name="wcet-dash-to-bigquery",
            task_id="wcet-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                "dbt-bigquery",
                "run",
                "--select",
                "dash_table__beervana dash_table_search__beervana",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            trigger_rule="all_done",
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )
        kube_dash_union = KubernetesPodOperator(
            name="wcet-dash-union-to-bigquery",
            task_id="wcet-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                "dbt-bigquery",
                "run",
                "--select",
                "dash_union__beervana",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            trigger_rule="all_done",
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )
        ga4_type=['session','goal']
        for type in ga4_type:
            kube_ga4 = KubernetesPodOperator(
                name="wcet-ga4-to-bigquery",
                task_id="wcet-ga4_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=[
                    "--environment=prod",
                    "run",
                    "tap-ga4",
                    "target-bigquery",
                    "dbt-bigquery:ga4_models",
                ],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_ga4(brand,type),
                get_logs=True,
            )
            kube_dash >> kube_ga4
        kube_dv360 = KubernetesPodOperator(
            name="wcet-dv360-to-bigquery",
            task_id="wcet-dv360_to_bigquery",
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
            env_vars=set_env_vars_dv360(brand),
            get_logs=True,
        )



        [kube_facebook, kube_tiktok, kube_dv360] >> kube_dash >> kube_dash_union
