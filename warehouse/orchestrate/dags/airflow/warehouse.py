"""Airflow DAGs for warehouse Meltano pipeline."""

import datetime
from copy import deepcopy
from datetime import timedelta

import pendulum
from airflow import models
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from comparison_package import ComparisonTrigger
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from kubernetes.client import models as k8s_models

IMAGE = "australia-southeast1-docker.pkg.dev/warehouse-main/meltano/meltano-warehouse-main:prod"
PROJECT_NAME = "warehouse-main"

local_tz = pendulum.timezone("Pacific/Auckland")

default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "retry_delay": timedelta(minutes=30),
    "start_date": datetime.datetime(2026, 6, 24, tzinfo=local_tz),
}

comparison_start_date = (
    datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
).strftime("%Y-%m-%d")

KUBE_RESOURCES = k8s_models.V1ResourceRequirements(
    limits={"memory": "1000M", "cpu": "500m"},
)


def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_warehouse_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main", deserialize_json=True)
    meltano_env_ga4 = Variable.get("meltano_developer_ga4_main", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique, **meltano_env_ga4}
    meltano_env["START_DATE"] = (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
    ).strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def set_env_vars_facebook(brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"facebook_raw__{brand}"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"facebook_transformed__{brand}"
    account_key = f"TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_{brand}_ID"
    if account_key in env:
        env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"] = env[account_key]
    return env


def set_env_vars_dv360(brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"dv360_raw__{brand}"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"dv360_transformed__{brand}"
    advertiser_key = f"TAP_DV360_ADVERTISER_{brand}_ID"
    if advertiser_key in env:
        env["TAP_DV360_ADVERTISER_ID"] = env[advertiser_key]
    return env


def set_env_vars_cm360(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"cm360_transformed__{brand}"
    return env


def set_env_vars_google_ads(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"google_ads_search_transformed__{brand}"
    return env


def set_env_vars_ga4(brand, goal):
    env = get_meltano_env()
    if goal == "session":
        env["TAP_GA4_REPORTS"] = "./report_sessions.json"
        env["GA4_GOAL"] = "session_goal"
    elif goal == "keyword":
        env["TAP_GA4_REPORTS"] = "./report_keyword.json"
        env["GA4_GOAL"] = "keyword_goal"
    else:
        env["TAP_GA4_REPORTS"] = "./report.json"
        env["GA4_GOAL"] = "goal"
    env["BQ_DATASET"] = f"ga4_raw__{brand}"
    env["BQ_METHOD"] = "gcs_stage"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
    env["DBT_BIGQUERY_DATASET"] = f"ga4_transformed__{brand}"
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
    property_key = f"TAP_GA4_PROPERTY_{brand.upper()}_ID"
    if property_key in env:
        env["TAP_GA4_PROPERTY_ID"] = env[property_key]
    return env


def set_env_vars_ga4_final(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"ga4_transformed__{brand}"
    return env


def set_env_vars_dash(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"dash_table__{brand}"
    return env


def set_env_vars_dash_search(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"dash_table_search__{brand}"
    return env


def make_facebook_comparison_check(brand):
    def facebook_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table=f"facebook_transformed__{brand}",
            table_name=f"facebook__{brand}",
            source_name="meta",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_warehouse_main",
            project_id=env["PROJECT_ID"],
            brand=brand,
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError(
                f"Facebook data accuracy check failed for {brand} — "
                "BQ data does not match source API."
            )
        return result

    return facebook_comparison_check


def make_dv360_comparison_standard_check(brand):
    def dv360_comparison_standard_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table=f"dv360_transformed__{brand}",
            table_name=f"dv360_standard__{brand}",
            source_name="dv360_standard",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_warehouse_main",
            project_id=env["PROJECT_ID"],
            brand=brand,
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError(
                f"DV360 standard data accuracy check failed for {brand} — "
                "BQ data does not match source API."
            )
        return result

    return dv360_comparison_standard_check


def make_dv360_comparison_youtube_check(brand):
    def dv360_comparison_youtube_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table=f"dv360_transformed__{brand}",
            table_name=f"dv360_youtube__{brand}",
            source_name="dv360_youtube",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_warehouse_main",
            project_id=env["PROJECT_ID"],
            brand=brand,
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError(
                f"DV360 YouTube data accuracy check failed for {brand} — "
                "BQ data does not match source API."
            )
        return result

    return dv360_comparison_youtube_check


# ---------------------------------------------------------------------------
# DAG 1: Google Ads + GA4 (schedule: 14:00 NZST daily)
# Flow: google_ads >> dash >> dash_search >> dash_union
#       >> ga4 goal/session/keyword >> ga4_final
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="warehouse-google-ads-ga4",
    schedule_interval="0 14 * * *",
    default_args=default_args,
    tags=["warehouse", "meltano", "google-ads", "ga4"],
) as dag_google_ads_ga4:
    brands = ["twh", "twhs"]

    for brand in brands:
        kube_google_ads = KubernetesPodOperator(
            name=f"warehouse-{brand}-google-ads-to-bigquery",
            task_id=f"warehouse-google-ads__{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                f"dbt-bigquery:google_ads_{brand}_models",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_google_ads(brand),
            get_logs=True,
        )

        kube_dash = KubernetesPodOperator(
            name=f"warehouse-{brand}-dash-to-bigquery",
            task_id=f"warehouse-dash__{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule="all_done",
            arguments=[
                "--environment=prod",
                "invoke",
                "dbt-bigquery",
                "run",
                "--select",
                f"dash_table__{brand}",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        kube_dash_search = KubernetesPodOperator(
            name=f"warehouse-{brand}-dash-search-to-bigquery",
            task_id=f"warehouse-dash_search__{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                "dbt-bigquery",
                "run",
                "--select",
                f"dash_table_search__{brand}",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash_search(brand),
            get_logs=True,
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"warehouse-{brand}-dash-union-to-bigquery",
            task_id=f"warehouse-dash_union__{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                "dbt-bigquery",
                "run",
                "--select",
                f"dash_union__{brand}",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        kube_ga4_list = []
        for goal in ["goal", "session", "keyword"]:
            kube_ga4 = KubernetesPodOperator(
                name=f"warehouse-{brand}-{goal}-ga4-to-bigquery",
                task_id=f"warehouse-ga4__{brand}_{goal}_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=[
                    "--environment=prod",
                    "run",
                    "tap-ga4",
                    "target-bigquery",
                    f"dbt-bigquery:ga4_{brand}_{goal}_models",
                ],
                container_resources=KUBE_RESOURCES,
                env_vars=set_env_vars_ga4(brand, goal),
                get_logs=True,
            )
            kube_ga4_list.append(kube_ga4)

        kube_ga4_final = KubernetesPodOperator(
            name=f"warehouse-{brand}-ga4-final-to-bigquery",
            task_id=f"warehouse-ga4_final__{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                f"dbt-bigquery:ga4_{brand}_final_models",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_ga4_final(brand),
            get_logs=True,
        )

        (
            kube_google_ads
            >> kube_dash
            >> kube_dash_search
            >> kube_dash_union
            >> kube_ga4_list
            >> kube_ga4_final
        )


# ---------------------------------------------------------------------------
# DAG 2: Social / Display / Programmatic (schedule: 05:00 NZST daily)
# Flow: [facebook, dv360, cm360] >> dash >> dash_search >> dash_union
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="warehouse-social-display-programmatic",
    schedule_interval="0 5 * * *",
    default_args=default_args,
    tags=["warehouse", "meltano", "social", "display"],
) as dag_social:
    brands = ["twh", "twhs"]

    for brand in brands:
        kube_facebook = KubernetesPodOperator(
            name=f"warehouse-{brand}-facebook-to-bigquery",
            task_id=f"warehouse-facebook__{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "run",
                "tap-facebook",
                "target-bigquery",
                f"dbt-bigquery:facebook_{brand}_models",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_facebook(brand),
            get_logs=True,
        )

        task_facebook_comparison = PythonOperator(
            task_id=f"warehouse-facebook_comparison__{brand}",
            python_callable=make_facebook_comparison_check(brand),
            retries=0,
            trigger_rule="all_done",
        )

        kube_dv360 = KubernetesPodOperator(
            name=f"warehouse-{brand}-dv360-to-bigquery",
            task_id=f"warehouse-dv360__{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "run",
                "tap-dv360",
                "target-bigquery",
                f"dbt-bigquery:dv360_{brand}_models",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dv360(brand),
            get_logs=True,
        )

        task_dv360_comparison_standard = PythonOperator(
            task_id=f"warehouse-dv360_comparison_standard__{brand}",
            python_callable=make_dv360_comparison_standard_check(brand),
            retries=0,
            trigger_rule="all_done",
        )

        task_dv360_comparison_youtube = PythonOperator(
            task_id=f"warehouse-dv360_comparison_youtube__{brand}",
            python_callable=make_dv360_comparison_youtube_check(brand),
            retries=0,
            trigger_rule="all_done",
        )

        kube_cm360 = KubernetesPodOperator(
            name=f"warehouse-{brand}-cm360-to-bigquery",
            task_id=f"warehouse-cm360__{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                f"dbt-bigquery:cm360_{brand}_models",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_cm360(brand),
            get_logs=True,
        )

        kube_dash = KubernetesPodOperator(
            name=f"warehouse-{brand}-dash-to-bigquery",
            task_id=f"warehouse-dash__{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule="all_done",
            arguments=[
                "--environment=prod",
                "invoke",
                "dbt-bigquery",
                "run",
                "--select",
                f"dash_table__{brand}",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        kube_dash_search = KubernetesPodOperator(
            name=f"warehouse-{brand}-dash-search-to-bigquery",
            task_id=f"warehouse-dash_search__{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                "dbt-bigquery",
                "run",
                "--select",
                f"dash_table_search__{brand}",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash_search(brand),
            get_logs=True,
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"warehouse-{brand}-dash-union-to-bigquery",
            task_id=f"warehouse-dash_union__{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                "dbt-bigquery",
                "run",
                "--select",
                f"dash_union__{brand}",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        kube_facebook >> task_facebook_comparison
        kube_dv360 >> [task_dv360_comparison_standard, task_dv360_comparison_youtube]

        (
            [kube_facebook, kube_dv360, kube_cm360]
            >> kube_dash
            >> kube_dash_search
            >> kube_dash_union
        )
