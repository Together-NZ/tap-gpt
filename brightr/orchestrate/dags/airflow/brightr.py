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


IMAGE = "australia-southeast1-docker.pkg.dev/brightr-main/meltano/meltano-brightr-main:prod"
PROJECT_NAME = "brightr-main"
COMPARISON_SECRET = "airflow-variables-meltano_brightr_main"

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
    meltano_env_unique = Variable.get("meltano_brightr_main", deserialize_json=True)
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


def get_ttd_start_date():
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


def set_env_vars_linkedin():
    env = get_meltano_env()
    env["BQ_DATASET"] = "linkedin_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "linkedin_transformed"
    return env


def set_env_vars_hivestack():
    env = get_meltano_env()
    env["BQ_DATASET"] = "hivestack_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "hivestack_transformed"
    return env


def set_env_vars_cm360():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "cm360_transformed"
    return env


def set_env_vars_ttd():
    env = get_meltano_env()
    env["BQ_DATASET"] = "ttd_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "ttd_transformed"
    env["TAP_TTD_START_DATE"] = get_ttd_start_date()
    return env


def set_env_vars_google_ads():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "google_ads_search_transformed"
    return env


def set_env_vars_dash():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "dash_table"
    env["PLAN_CODE"] = "brightr"
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
# DAG 1: Social / display / programmatic
# Flow: CM360 -> TTD; platforms -> dash. Comparisons follow source transforms.
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="brightr-meltano-extraction-transformation-dbt",
    schedule_interval="0 4 * * *",
    default_args=default_args,
) as dag:
    kube_facebook = KubernetesPodOperator(
        name="brightr-facebook-to-bigquery",
        task_id="brightr-facebook_to_bigquery",
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
    kube_linkedin = KubernetesPodOperator(
        name="brightr-linkedin-to-bigquery",
        task_id="brightr-linkedin_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-linkedin-ads",
            "target-bigquery",
            "dbt-bigquery:linkedin_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_linkedin(),
        get_logs=True,
    )
    kube_hivestack = KubernetesPodOperator(
        name="brightr-hivestack-to-bigquery",
        task_id="brightr-hivestack_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-hivestack",
            "target-bigquery",
            "dbt-bigquery:hivestack_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_hivestack(),
        get_logs=True,
    )
    kube_cm360 = KubernetesPodOperator(
        name="brightr-cm360-to-bigquery",
        task_id="brightr-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360(),
        get_logs=True,
    )
    kube_ttd = KubernetesPodOperator(
        name="brightr-ttd-to-bigquery",
        task_id="brightr-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-ttd",
            "target-bigquery",
            "dbt-bigquery:ttd_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ttd(),
        get_logs=True,
        execution_timeout=timedelta(minutes=60),
    )
    kube_dash = KubernetesPodOperator(
        name="brightr-dash-to-bigquery",
        task_id="brightr-dash_to_bigquery",
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

    def linkedin_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="linkedin_transformed",
            table_name="linkedin",
            source_name="linkedin",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name=COMPARISON_SECRET,
            project_id=env["PROJECT_ID"],
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError(
                "LinkedIn data accuracy check failed — BigQuery does not match the source API."
            )
        return result

    task_facebook_comparison = PythonOperator(
        task_id="task_facebook_comparison",
        python_callable=facebook_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    task_linkedin_comparison = PythonOperator(
        task_id="task_linkedin_comparison",
        python_callable=linkedin_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )

    kube_facebook >> task_facebook_comparison
    kube_linkedin >> task_linkedin_comparison
    kube_cm360 >> kube_ttd
    [kube_facebook, kube_linkedin, kube_hivestack, kube_cm360, kube_ttd] >> kube_dash


# ---------------------------------------------------------------------------
# DAG 2: Google Ads / dash search / GA4
# Flow: Google Ads -> [dash, dash_search] -> union -> GA4 -> GA4 final.
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="brightr-meltano-google-ads",
    schedule_interval="20 13 * * *",
    default_args=default_args,
) as google_dag:
    kube_google_ads = KubernetesPodOperator(
        name="brightr-google-ads-to-bigquery",
        task_id="brightr-google_ads_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:google_ads_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_google_ads(),
        get_logs=True,
    )
    kube_dash = KubernetesPodOperator(
        name="brightr-dash-to-bigquery",
        task_id="brightr-dash_to_bigquery",
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
        name="brightr-dash-search-to-bigquery",
        task_id="brightr-dash_search_to_bigquery",
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
        name="brightr-dash-union-to-bigquery",
        task_id="brightr-dash_union_to_bigquery",
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
        name="brightr-ga4-final-to-bigquery",
        task_id="brightr-ga4_final_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:ga4_final_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4_final(),
        get_logs=True,
    )

    ga4_tasks = []
    for goal in ["goal", "session", "keyword"]:
        kube_ga4 = KubernetesPodOperator(
            name=f"brightr-ga4-{goal}-to-bigquery",
            task_id=f"brightr-ga4_{goal}_to_bigquery",
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

    kube_google_ads >> [kube_dash, kube_dash_search]
    [kube_dash, kube_dash_search] >> kube_dash_union
    for task in ga4_tasks:
        kube_dash_union >> task >> kube_ga4_final
