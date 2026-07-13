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

IMAGE = "australia-southeast1-docker.pkg.dev/wendys-main/meltano/meltano-wendys-main:prod"
PROJECT_NAME = "wendys-main"

local_tz = pendulum.timezone("Pacific/Auckland")

default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "retry_delay": timedelta(minutes=30),
    "start_date": datetime.datetime(2026, 7, 12, tzinfo=local_tz),
}

comparison_start_date = (
    datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
).strftime("%Y-%m-%d")

KUBE_RESOURCES = k8s_models.V1ResourceRequirements(
    limits={"memory": "1000M", "cpu": "500m"},
)


def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_wendys_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main", deserialize_json=True)
    meltano_env_ga4 = Variable.get("meltano_developer_ga4_main", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique, **meltano_env_ga4}
    meltano_env["START_DATE"] = (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
    ).strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def set_env_vars_google_ads():
    env = get_meltano_env()
    env["BQ_DATASET"] = "google_ads_search"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "google_ads_search_transformed"
    return env


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
    elif goal == "keyword":
        env["TAP_GA4_REPORTS"] = "./report_keyword.json"
        env["GA4_GOAL"] = "keyword_goal"
    else:
        env["TAP_GA4_REPORTS"] = "./report.json"
        env["GA4_GOAL"] = "goal"
    env["BQ_DATASET"] = "ga4_raw"
    env["BQ_METHOD"] = "gcs_stage"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
    env["DBT_BIGQUERY_DATASET"] = "ga4_transformed"
    env["PLAN_CODE_GA4"] = "wendys"
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
    if "TAP_GA4_PROPERTY_ID" in env:
        pass
    return env


def set_env_vars_ga4_final():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "ga4_transformed"
    env["PLAN_CODE_GA4"] = "wendys"
    return env


def set_env_vars_facebook():
    env = get_meltano_env()
    env["BQ_DATASET"] = "facebook_raw"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "facebook_transformed"
    return env


def set_env_vars_dv360():
    env = get_meltano_env()
    env["BQ_DATASET"] = "dv360_raw"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "dv360_transformed"
    return env


def set_env_vars_cm360():
    env = get_meltano_env()
    env["BQ_DATASET"] = "cm360_raw"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "cm360_transformed"
    return env


def set_env_vars_ttd():
    env = get_meltano_env()
    env["BQ_DATASET"] = "ttd_raw"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "ttd_transformed"
    env["TAP_TTD_START_DATE"] = get_ttd_start_date()
    return env


def set_env_vars_hivestack():
    env = get_meltano_env()
    env["BQ_DATASET"] = "hivestack_raw"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "hivestack_transformed"
    return env


def set_env_vars_snapchat():
    env = get_meltano_env()
    env["BQ_DATASET"] = "snapchat_raw"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "snapchat_transformed"
    return env


def set_env_vars_tiktok():
    env = get_meltano_env()
    env["BQ_DATASET"] = "tiktok_raw"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "tiktok_transformed"
    return env


# ---------------------------------------------------------------------------
# DAG 1: Google Ads + GA4 (schedule: 14:00 NZST daily)
# Flow: google_ads >> dash >> dash_search >> dash_union >> ga4 (goal, session, keyword) >> ga4_final
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="wendys-google-ads-ga4",
    schedule_interval="0 14 * * *",
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=80),
) as dag_google:

    kube_google_ads = KubernetesPodOperator(
        name="wendys-google-ads-to-bigquery",
        task_id="wendys-google-ads_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:google_ads_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_google_ads(),
        get_logs=True,
    )

    kube_dash = KubernetesPodOperator(
        name="wendys-dash-to-bigquery",
        task_id="wendys-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule="all_done",
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_table"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash(),
    )

    kube_dash_search = KubernetesPodOperator(
        name="wendys-dash-search-to-bigquery",
        task_id="wendys-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "+dash_table_search"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash_search(),
    )

    kube_dash_union = KubernetesPodOperator(
        name="wendys-dash-union-to-bigquery",
        task_id="wendys-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_union"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash(),
    )

    kube_ga4_final = KubernetesPodOperator(
        name="wendys-ga4-final-to-bigquery",
        task_id="wendys-ga4_final_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:ga4_final_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_ga4_final(),
        get_logs=True,
    )
    kube_tiktok = KubernetesPodOperator(
        name="wendys-tiktok-to-bigquery",
        task_id="wendys-tiktok_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-tiktok",
            "target-bigquery",
            "--full-refresh",
            "dbt-bigquery:tiktok_models",
        ],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_tiktok(),
    )

    def tiktok_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="tiktok_transformed",
            table_name="tiktok",
            source_name="tiktok",
            start_date=comparison_start_date,
            end_date=(datetime.datetime.now(local_tz) - timedelta(days=1)).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_wendys_main",
            project_id=env["PROJECT_ID"],
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("Tiktok data accuracy check failed — BQ data does not match source API.")
        return result
    task_tiktok_comparison = PythonOperator(
        task_id="task_tiktok_comparison",
        python_callable=tiktok_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    kube_tiktok >> task_tiktok_comparison
    kube_ga4_list = []
    for goal in ["goal", "session", "keyword"]:
        kube_ga4 = KubernetesPodOperator(
            name=f"wendys-ga4-{goal}-to-bigquery",
            task_id=f"wendys-ga4_{goal}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "run",
                "tap-ga4",
                "target-bigquery",
                f"dbt-bigquery:ga4_{goal}_models",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_ga4(goal),
            get_logs=True,
        )
        kube_ga4_list.append(kube_ga4)

    for task in kube_ga4_list:
        kube_dash_union >> task >> kube_ga4_final

    kube_google_ads >> kube_tiktok >> kube_dash >> kube_dash_search >> kube_dash_union


# ---------------------------------------------------------------------------
# DAG 2: Social / Display / Programmatic (schedule: 05:00 NZST daily)
# Flow: [facebook, dv360, cm360, ttd, hivestack, snapchat, tiktok] >> dash >> dash_search >> dash_union
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="wendys-social-display-programmatic",
    schedule_interval="0 5 * * *",
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=80),
) as dag_social:

    kube_facebook = KubernetesPodOperator(
        name="wendys-facebook-to-bigquery",
        task_id="wendys-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery", "dbt-bigquery:facebook_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_facebook(),
    )

    kube_dv360 = KubernetesPodOperator(
        name="wendys-dv360-to-bigquery",
        task_id="wendys-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery", "dbt-bigquery:dv360_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dv360(),
    )

    kube_cm360 = KubernetesPodOperator(
        name="wendys-cm360-to-bigquery",
        task_id="wendys-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_cm360(),
    )

    kube_ttd = KubernetesPodOperator(
        name="wendys-ttd-to-bigquery",
        task_id="wendys-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery", "dbt-bigquery:ttd_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_ttd(),
        execution_timeout=timedelta(minutes=60),
    )

    kube_hivestack = KubernetesPodOperator(
        name="wendys-hivestack-to-bigquery",
        task_id="wendys-hivestack_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery", "dbt-bigquery:hivestack_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_hivestack(),
    )

    kube_snapchat = KubernetesPodOperator(
        name="wendys-snapchat-to-bigquery",
        task_id="wendys-snapchat_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-snapchat-ads", "target-bigquery", "dbt-bigquery:snapchat_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_snapchat(),
    )

 

    kube_dash = KubernetesPodOperator(
        name="wendys-dash-to-bigquery",
        task_id="wendys-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule="all_done",
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_table"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash(),
    )

    kube_dash_search = KubernetesPodOperator(
        name="wendys-dash-search-to-bigquery",
        task_id="wendys-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "+dash_table_search"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash_search(),
    )

    kube_dash_union = KubernetesPodOperator(
        name="wendys-dash-union-to-bigquery",
        task_id="wendys-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_union"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash(),
    )

 

    def snapchat_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="snapchat_transformed",
            table_name="snapchat",
            source_name="snapchat",
            start_date=comparison_start_date,
            end_date=(datetime.datetime.now(local_tz) + timedelta(days=1)).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_wendys_main",
            project_id=env["PROJECT_ID"],
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("Snapchat data accuracy check failed — BQ data does not match source API.")
        return result

    def dv360_standard_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="dv360_transformed",
            table_name="dv360_standard",
            source_name="dv360_standard",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_wendys_main",
            project_id=env["PROJECT_ID"],
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("DV360 data accuracy check failed — BQ data does not match source API.")
        return result

    def dv360_youtube_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="dv360_transformed",
            table_name="dv360_youtube",
            source_name="dv360_youtube",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_wendys_main",
            project_id=env["PROJECT_ID"],
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("DV360 data accuracy check failed — BQ data does not match source API.")
        return result

    def facebook_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="facebook_transformed",
            table_name="facebook",
            source_name="meta",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_wendys_main",
            project_id=env["PROJECT_ID"],
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("Facebook data accuracy check failed — BQ data does not match source API.")
        return result

    task_snapchat_comparison = PythonOperator(
        task_id="task_snapchat_comparison",
        python_callable=snapchat_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    task_dv360_standard_comparison = PythonOperator(
        task_id="task_dv360_standard_comparison",
        python_callable=dv360_standard_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    task_dv360_youtube_comparison = PythonOperator(
        task_id="task_dv360_youtube_comparison",
        python_callable=dv360_youtube_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    task_facebook_comparison = PythonOperator(
        task_id="task_facebook_comparison",
        python_callable=facebook_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )

    kube_cm360 >> kube_ttd
    kube_cm360 >> kube_dv360
    kube_facebook >> task_facebook_comparison
    kube_dv360 >> [task_dv360_standard_comparison, task_dv360_youtube_comparison]
    kube_snapchat >> task_snapchat_comparison
    [
        kube_facebook,
        kube_dv360,
        kube_ttd,
        kube_hivestack,
        kube_snapchat
    ] >> kube_dash >> kube_dash_search >> kube_dash_union
