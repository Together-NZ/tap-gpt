import datetime
import logging
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


IMAGE = "australia-southeast1-docker.pkg.dev/best-start-main/meltano/meltano-best-start-main:prod"
PROJECT_NAME = "best-start-main"
COMPARISON_SECRET = "airflow-variables-meltano_beststart_main"
BRANDS = ["beststart", "hr_career"]

log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
comparison_start_date = (
    datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
).strftime("%Y-%m-%d")

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz),
}


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_beststart_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=13)
    ).strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def get_ga4_start_date():
    return (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
    ).strftime("%Y-%m-%d")


def set_env_vars_facebook():
    env = get_meltano_env()
    env["BQ_DATASET"] = "facebook_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "facebook_transformed"
    return env


def set_env_vars_tiktok():
    env = get_meltano_env()
    env["BQ_DATASET"] = "tiktok_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "tiktok_transformed"
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


def set_env_vars_google_ads_search(label):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"google_ads_search_transformed__{label}"
    return env


def set_env_vars_dash():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "dash_table"
    env["PLAN_CODE"] = "bs"
    return env


def set_env_vars_dash_search(label):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"dash_table_search__{label}"
    return env


def set_env_vars_dash_search_union():
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
    env["DBT_BIGQUERY_DATASET"] = "ga4_transformed"
    developer_creds = Credentials(
        None,
        refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
        client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
    )
    developer_creds.refresh(Request())
    env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
    env["TAP_GA4_START_DATE"] = get_ga4_start_date()
    return env


def set_env_vars_ga4_final():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "ga4_transformed"
    return env


# ---------------------------------------------------------------------------
# DAG 1: Social / display
# Flow: CM360 -> DV360; platforms -> dash. Comparisons after source transforms.
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="beststart-meltano-extraction-transformation-dbt",
    schedule_interval="0 4 * * *",
    default_args=default_args,
) as dag:
    kube_facebook = KubernetesPodOperator(
        name="beststart-facebook-to-bigquery",
        task_id="beststart-facebook_to_bigquery",
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
    kube_tiktok = KubernetesPodOperator(
        name="beststart-tiktok-to-bigquery",
        task_id="beststart-tiktok_to_bigquery",
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
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_tiktok(),
        get_logs=True,
    )
    kube_cm360 = KubernetesPodOperator(
        name="beststart-cm360-to-bigquery",
        task_id="beststart-cm360_to_bigquery",
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
        name="beststart-dv360-to-bigquery",
        task_id="beststart-dv360_to_bigquery",
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
        name="beststart-dash-to-bigquery",
        task_id="beststart-dash_to_bigquery",
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

    def facebook_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="facebook_transformed",
            table_name="facebook",
            source_name="meta",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name=COMPARISON_SECRET,
            project_id=env["PROJECT_ID"],
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError(
                "Facebook data accuracy check failed — BigQuery does not match the source API."
            )
        return result

    def tiktok_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="tiktok_transformed",
            table_name="tiktok",
            source_name="tiktok",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name=COMPARISON_SECRET,
            project_id=env["PROJECT_ID"],
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError(
                "TikTok data accuracy check failed — BigQuery does not match the source API."
            )
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
            secret_name=COMPARISON_SECRET,
            project_id=env["PROJECT_ID"],
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError(
                "DV360 standard data accuracy check failed — BigQuery does not match the source API."
            )
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
            secret_name=COMPARISON_SECRET,
            project_id=env["PROJECT_ID"],
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError(
                "DV360 YouTube data accuracy check failed — BigQuery does not match the source API."
            )
        return result

    task_facebook_comparison = PythonOperator(
        task_id="task_facebook_comparison",
        python_callable=facebook_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    task_tiktok_comparison = PythonOperator(
        task_id="task_tiktok_comparison",
        python_callable=tiktok_comparison_check,
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

    kube_facebook >> task_facebook_comparison
    kube_tiktok >> task_tiktok_comparison
    kube_cm360 >> kube_dv360
    kube_dv360 >> [task_dv360_standard_comparison, task_dv360_youtube_comparison]
    [kube_facebook, kube_tiktok, kube_cm360, kube_dv360] >> kube_dash


# ---------------------------------------------------------------------------
# DAG 2: Google Ads / dash search / GA4
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="beststart-meltano-google-ads",
    schedule_interval="00 14 * * *",
    default_args=default_args,
) as google_dag:
    kube_dash = KubernetesPodOperator(
        name="beststart-dash-to-bigquery",
        task_id="beststart-dash_to_bigquery",
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
    kube_dash_search_union = KubernetesPodOperator(
        name="beststart-dash-search-union-to-bigquery",
        task_id="beststart-dash_search_union_to_bigquery",
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
        env_vars=set_env_vars_dash_search_union(),
        get_logs=True,
    )
    kube_dash_union = KubernetesPodOperator(
        name="beststart-dash-union-to-bigquery",
        task_id="beststart-dash_union_to_bigquery",
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
        name="beststart-ga4-final-to-bigquery",
        task_id="beststart-ga4_final_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:ga4_final_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4_final(),
        get_logs=True,
    )

    google_ads_tasks = []
    dash_search_tasks = []
    for label in BRANDS:
        kube_google_ads = KubernetesPodOperator(
            name=f"beststart-google-ads-search-to-bigquery-{label}",
            task_id=f"beststart-google_ads_search_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                f"dbt-bigquery:google_ads_{label}_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(label),
            get_logs=True,
        )
        kube_dash_search = KubernetesPodOperator(
            name=f"beststart-dash-search-to-bigquery-{label}",
            task_id=f"beststart-dash_search_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                "dbt-bigquery",
                "run",
                "--select",
                f"dash_table_search__{label}",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(label),
            get_logs=True,
        )
        kube_google_ads >> kube_dash_search
        google_ads_tasks.append(kube_google_ads)
        dash_search_tasks.append(kube_dash_search)

    ga4_tasks = []
    for goal in ["goal", "session", "keyword"]:
        kube_ga4 = KubernetesPodOperator(
            name=f"beststart-ga4-{goal}-to-bigquery",
            task_id=f"beststart-ga4_{goal}_to_bigquery",
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
        ga4_tasks.append(kube_ga4)

    google_ads_tasks >> kube_dash
    dash_search_tasks >> kube_dash_search_union
    [kube_dash, kube_dash_search_union] >> kube_dash_union
    for task in ga4_tasks:
        kube_dash_union >> task >> kube_ga4_final
