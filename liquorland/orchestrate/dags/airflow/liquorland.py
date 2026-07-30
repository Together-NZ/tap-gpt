import datetime
import logging
from copy import deepcopy

import pendulum
from airflow import models
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from comparison_package import ComparisonTrigger
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from kubernetes.client import models as k8s_models

IMAGE = "australia-southeast1-docker.pkg.dev/liquorland-main/meltano/meltano-liquorland-main:prod"
PROJECT_NAME = "liquorland-main"
COMPARISON_SECRET = "airflow-variables-meltano_liquorland_main"

log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
comparison_start_date = (
    datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
).strftime("%Y-%m-%d")

default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "retry_delay": datetime.timedelta(minutes=30),
    "start_date": datetime.datetime(2024, 1, 1, tzinfo=local_tz),
}

KUBE_RESOURCES = k8s_models.V1ResourceRequirements(
    limits={"memory": "1000M", "cpu": "500m"},
)


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_liquorland_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    # Match Contact: ~30d so GA4 tap covers the same window as transforms
    meltano_env["START_DATE"] = (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=29)
    ).strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# DAG 1: Social / Display / Programmatic (schedule: 05:00 NZST daily)
# Flow: CM360 >> [DV360, TTD]; platforms >> dash >> dash_union
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="liquorland-meltano-extraction-transformation-dbt",
    schedule_interval="0 5 * * *",
    default_args=default_args,
) as dag:

    def set_env_vars_cm360():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "cm360_transformed"
        return env

    def set_env_vars_facebook():
        env = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "facebook_transformed"
        return env

    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "dash_table"
        return env

    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "dv360_transformed"
        return env

    def set_env_vars_ttd():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "ttd_transformed"
        env["TAP_TTD_START_DATE"] = get_ttd_start_date()
        return env

    def set_env_vars_hivestack():
        env = get_meltano_env()
        env["BQ_DATASET"] = "hivestack_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "hivestack_transformed"
        return env

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
            raise ValueError("Facebook data accuracy check failed — BQ data does not match source API.")
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
            raise ValueError("DV360 standard data accuracy check failed — BQ data does not match source API.")
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
            raise ValueError("DV360 YouTube data accuracy check failed — BQ data does not match source API.")
        return result

    kube_cm360 = KubernetesPodOperator(
        name="liquorland-cm360-to-bigquery",
        task_id="liquorland-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_cm360(),
        get_logs=True,
    )

    kube_facebook = KubernetesPodOperator(
        name="liquorland-facebook-to-bigquery",
        task_id="liquorland-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery", "dbt-bigquery:facebook_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_facebook(),
        get_logs=True,
    )

    kube_dv360 = KubernetesPodOperator(
        name="liquorland-dv360-to-bigquery",
        task_id="liquorland-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery", "dbt-bigquery:dv360_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dv360(),
        get_logs=True,
    )

    kube_ttd = KubernetesPodOperator(
        name="liquorland-ttd-to-bigquery",
        task_id="liquorland-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery", "dbt-bigquery:ttd_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_ttd(),
        get_logs=True,
    )

    kube_hivestack = KubernetesPodOperator(
        name="liquorland-hivestack-to-bigquery",
        task_id="liquorland-hivestack_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery", "dbt-bigquery:hivestack_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_hivestack(),
        get_logs=True,
    )

    kube_dash = KubernetesPodOperator(
        name="liquorland-dash-to-bigquery",
        task_id="liquorland-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule="all_done",
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_table"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash(),
    )

    kube_dash_union = KubernetesPodOperator(
        name="liquorland-dash-union-to-bigquery",
        task_id="liquorland-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_union"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash(),
    )

    task_facebook_comparison = PythonOperator(
        task_id="task_facebook_comparison",
        python_callable=facebook_comparison_check,
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

    # CM360 before DV360/TTD; social-only closes with dash → dash_union (no search)
    kube_facebook >> task_facebook_comparison
    kube_cm360 >> [kube_dv360, kube_ttd]
    kube_dv360 >> [task_dv360_standard_comparison, task_dv360_youtube_comparison]
    [kube_facebook, kube_cm360, kube_dv360, kube_ttd, kube_hivestack] >> kube_dash >> kube_dash_union


# ---------------------------------------------------------------------------
# DAG 2: Google Ads + GA4 (schedule: 14:00 NZST daily)
# Flow: google_ads >> [dash, dash_search] >> dash_union >> [goal, session, keyword] >> ga4_final
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="liquorland-meltano-google_ads",
    schedule_interval="0 14 * * *",
    default_args=default_args,
) as google_dag:

    def set_env_vars_google_ads_search():
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads_search"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "google_ads_search_transformed"
        return env

    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "dash_table"
        return env

    def set_env_vars_dash_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "dash_table_search"
        return env

    def set_env_vars_ga4_final():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = "ga4_transformed"
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
        developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
        developer_creds.refresh(Request())
        # tap-ga4 reads START_DATE; keep explicit 30d aligned with get_ga4_start_date()
        env["START_DATE"] = get_ga4_start_date()
        env["TAP_GA4_START_DATE"] = get_ga4_start_date()
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        return env

    kube_google_ads_search = KubernetesPodOperator(
        name="liquorland-google-ads-search-to-bigquery",
        task_id="liquorland-google-ads-search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:google_ads_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_google_ads_search(),
        get_logs=True,
    )

    kube_dash = KubernetesPodOperator(
        name="liquorland-dash-to-bigquery",
        task_id="liquorland-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule="all_done",
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_table"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash(),
    )

    kube_dash_search = KubernetesPodOperator(
        name="liquorland-dash-search-to-bigquery",
        task_id="liquorland-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_table_search"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash_search(),
    )

    kube_dash_union = KubernetesPodOperator(
        name="liquorland-dash-union-to-bigquery",
        task_id="liquorland-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_union"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash(),
    )

    kube_ga4_final = KubernetesPodOperator(
        name="liquorland-ga4-final-to-bigquery",
        task_id="liquorland-ga4_final_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:ga4_final_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_ga4_final(),
        get_logs=True,
    )

    kube_ga4_list = []
    for goal in ["goal", "session", "keyword"]:
        kube_ga4_t = KubernetesPodOperator(
            name=f"liquorland-{goal}-ga4-to-bigquery",
            task_id=f"liquorland-{goal}-ga4_to_bigquery",
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
        kube_ga4_list.append(kube_ga4_t)

    kube_google_ads_search >> [kube_dash, kube_dash_search]
    [kube_dash, kube_dash_search] >> kube_dash_union
    for task in kube_ga4_list:
        kube_dash_union >> task >> kube_ga4_final
