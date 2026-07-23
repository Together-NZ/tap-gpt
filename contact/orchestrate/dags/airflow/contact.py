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


IMAGE = "australia-southeast1-docker.pkg.dev/contact-energy-main/meltano/meltano-contact-energy-main:prod"
PROJECT_NAME = "contact-energy-main"
COMPARISON_SECRET = "airflow-variables-meltano_contact_main"

log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
comparison_start_date = (
    datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
).strftime("%Y-%m-%d")

BRANDS = ["mobile", "broadband", "energy"]

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz),
}


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_contact_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=13)
    meltano_env["START_DATE"] = yesterday.strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime(
        "%Y-%m-%d"
    )


def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime(
        "%Y-%m-%d"
    )


def _base_env(raw_dataset, transformed_dataset, brand=None):
    env = get_meltano_env()
    if raw_dataset:
        env["BQ_DATASET"] = raw_dataset
        env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = transformed_dataset
    if brand is not None:
        env["CONTACT_LABEL"] = brand
    return env


def set_env_vars_facebook():
    return _base_env("facebook_raw", "facebook_transformed")


def set_env_vars_dv360():
    return _base_env("dv360_raw", "dv360_transformed")


def set_env_vars_ttd():
    env = _base_env("ttd_raw", "ttd_transformed")
    env["TAP_TTD_START_DATE"] = get_ttd_start_date()
    return env


def set_env_vars_hivestack():
    return _base_env("hivestack_raw", "hivestack_transformed")


def set_env_vars_tiktok():
    return _base_env("tiktok_raw", "tiktok_transformed")


def set_env_vars_cm360():
    return _base_env("cm360_raw", "cm360_transformed")


def set_env_vars_google_ads():
    return _base_env("google_ads_search_raw", "google_ads_search_transformed")


def set_env_vars_dash():
    env = _base_env(None, "dash_table")
    env["PLAN_CODE"] = "con"
    return env


def set_env_vars_dash_search():
    env = _base_env(None, "dash_table_search")
    env["PLAN_CODE"] = "con"
    return env


def set_env_vars_dash_brand(brand):
    env = _base_env(None, f"dash_table__{brand}", brand)
    env["PLAN_CODE"] = "con"
    return env


def set_env_vars_ga4(goal):
    env = get_meltano_env()
    if goal == "session":
        env["TAP_GA4_REPORTS"] = "./report_sessions.json"
        env["GA4_GOAL"] = "session_goal"
    elif goal == "keyword":
        env["TAP_GA4_REPORTS"] = "./report_keyword.json"
        env["GA4_GOAL"] = "keyword_goal"
    elif goal == "ecommerce":
        env["TAP_GA4_REPORTS"] = "./ecommerce_report.json"
        env["GA4_GOAL"] = "ecommerce_goal"
    else:
        env["TAP_GA4_REPORTS"] = "./report.json"
        env["GA4_GOAL"] = "goal"
    env["BQ_DATASET"] = "ga4_raw"
    env["BQ_METHOD"] = "gcs_stage"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "ga4_transformed"
    env["PLAN_CODE"] = "con"
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
    env = _base_env(None, "ga4_transformed")
    env["PLAN_CODE"] = "con"
    return env


def set_env_vars_ga4_brand(brand):
    env = _base_env(None, f"ga4_transformed__{brand}", brand)
    env["PLAN_CODE"] = "con"
    return env


# ---------------------------------------------------------------------------
# DAG 1: Social / Display / Programmatic
# Extract + transform in one task (same pattern as Kiwibank)
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="contact-meltano-extraction-transformation-dbt",
    schedule_interval="0 5 * * *",
    default_args=default_args,
) as dag:
    kube_facebook = KubernetesPodOperator(
        name="contact-facebook-to-bq",
        task_id="contact-facebook_to_bigquery",
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
    kube_dv360 = KubernetesPodOperator(
        name="contact-dv360-to-bq",
        task_id="contact-dv360_to_bigquery",
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
    kube_ttd = KubernetesPodOperator(
        name="contact-ttd-to-bq",
        task_id="contact-ttd_to_bigquery",
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
    kube_hivestack = KubernetesPodOperator(
        name="contact-hivestack-to-bq",
        task_id="contact-hivestack_to_bigquery",
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
        name="contact-cm360-to-bq",
        task_id="contact-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360(),
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
                "Facebook data accuracy check failed — BQ data does not match source API."
            )
        return result

    def dv360_comparison_standard_check(**context):
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
                "DV360 standard data accuracy check failed — BQ data does not match source API."
            )
        return result

    def dv360_comparison_youtube_check(**context):
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
                "DV360 YouTube data accuracy check failed — BQ data does not match source API."
            )
        return result

    task_facebook_comparison = PythonOperator(
        task_id="task_facebook_comparison",
        python_callable=facebook_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    task_dv360_comparison_standard = PythonOperator(
        task_id="task_dv360_comparison_standard",
        python_callable=dv360_comparison_standard_check,
        retries=0,
        trigger_rule="all_done",
    )
    task_dv360_comparison_youtube = PythonOperator(
        task_id="task_dv360_comparison_youtube",
        python_callable=dv360_comparison_youtube_check,
        retries=0,
        trigger_rule="all_done",
    )

    kube_cm360 >> kube_dv360
    kube_cm360 >> kube_ttd
    kube_facebook >> task_facebook_comparison
    kube_dv360 >> [task_dv360_comparison_standard, task_dv360_comparison_youtube]


# ---------------------------------------------------------------------------
# DAG 2: Google Ads + TikTok + central dash → brand dash_union → GA4
# Extract + transform in one task (same pattern as Kiwibank)
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="contact-energy-google_ads_search",
    schedule_interval="0 14 * * *",
    default_args=default_args,
) as dag_google_ads:
    kube_tiktok = KubernetesPodOperator(
        name="contact-tiktok-to-bq",
        task_id="contact-tiktok_to_bigquery",
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
        env_vars=set_env_vars_tiktok(),
        get_logs=True,
    )
    kube_google_ads = KubernetesPodOperator(
        name="contact-google-ads-to-bq",
        task_id="contact-google_ads_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:google_ads_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_google_ads(),
        get_logs=True,
    )

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
                "Tiktok data accuracy check failed — BQ data does not match source API."
            )
        return result

    task_tiktok_comparison = PythonOperator(
        task_id="task_tiktok_comparison",
        python_callable=tiktok_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    kube_tiktok >> task_tiktok_comparison

    kube_dash = KubernetesPodOperator(
        name="contact-dash-to-bq",
        task_id="contact_dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule="all_done",
        arguments=["--environment=prod", "invoke", "dbt-bigquery:dash_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        get_logs=True,
    )
    kube_dash_search = KubernetesPodOperator(
        name="contact-dash-search-to-bq",
        task_id="contact_dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:dash_search_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search(),
        get_logs=True,
    )
    kube_dash_union = KubernetesPodOperator(
        name="contact-dash-union-to-bq",
        task_id="contact_dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:dash_union_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        get_logs=True,
    )

    [kube_google_ads, kube_tiktok] >> kube_dash
    kube_google_ads >> kube_dash_search
    [kube_dash, kube_dash_search] >> kube_dash_union

    ga4_task_list = []
    for goal in ["goal", "session", "keyword", "ecommerce"]:
        kube_ga4 = KubernetesPodOperator(
            name=f"contact-ga4-to-bq-{goal}",
            task_id=f"contact_ga4_to_bigquery_{goal}",
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
        ga4_task_list.append(kube_ga4)
        kube_dash_union >> kube_ga4

    kube_ga4_final = KubernetesPodOperator(
        name="contact-ga4-final-to-bq",
        task_id="contact_ga4_final_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:ga4_final_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4_final(),
        get_logs=True,
    )
    for task in ga4_task_list:
        task >> kube_ga4_final

    for brand in BRANDS:
        kube_brand_dash_union = KubernetesPodOperator(
            name=f"contact-{brand}-dash-union-to-bq",
            task_id=f"contact-{brand}_dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                f"dbt-bigquery:dash_union_{brand}_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_brand(brand),
            get_logs=True,
        )
        kube_ga4_brand = KubernetesPodOperator(
            name=f"contact-{brand}-ga4-channel-to-bq",
            task_id=f"contact-{brand}_ga4_goal_channel_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                f"dbt-bigquery:ga4_{brand}_final_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4_brand(brand),
            get_logs=True,
        )
        kube_ga4_keyword = KubernetesPodOperator(
            name=f"contact-{brand}-ga4-keyword-to-bq",
            task_id=f"contact-{brand}_ga4_keyword_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                f"dbt-bigquery:ga4_{brand}_keyword_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4_brand(brand),
            get_logs=True,
        )
        kube_ga4_ecommerce = KubernetesPodOperator(
            name=f"contact-{brand}-ga4-ecommerce-to-bq",
            task_id=f"contact-{brand}_ga4_ecommerce_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                f"dbt-bigquery:ga4_{brand}_ecommerce_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4_brand(brand),
            get_logs=True,
        )

        kube_dash_union >> kube_brand_dash_union
        kube_brand_dash_union >> [
            kube_ga4_brand,
            kube_ga4_keyword,
            kube_ga4_ecommerce,
        ]
        kube_ga4_final >> [
            kube_ga4_brand,
            kube_ga4_keyword,
            kube_ga4_ecommerce,
        ]
