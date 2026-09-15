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


IMAGE = "australia-southeast1-docker.pkg.dev/inghams-main/meltano/meltano-inghams-main:prod"
PROJECT_NAME = "inghams-main"
COMPARISON_SECRET = "airflow-variables-meltano_inghams_main"

BRANDS = ["inghams", "waitoa"]

log: logging.log = logging.getLogger("airflow.task")
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
    "retry_delay": timedelta(minutes=30),
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz),
}


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_inghams_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
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


def set_env_vars_facebook(account_id, brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"facebook_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"facebook_transformed__{brand}"
    env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"] = account_id
    return env


def set_env_vars_dv360(advertiser_id, brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"dv360_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"dv360_transformed__{brand}"
    env["DV360_ADVERTISER_ID"] = advertiser_id
    env["TAP_DV360_ADVERTISER_ID"] = advertiser_id
    return env


def set_env_vars_ttd(advertiser_id, brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"ttd_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"ttd_transformed__{brand}"
    env["TTD_ADVERTISER_ID"] = advertiser_id
    env["TAP_TTD_ADVERTISER_ID"] = advertiser_id
    env["TAP_TTD_START_DATE"] = get_ttd_start_date()
    env["REFERENCE_CM360_TRANSFORMED_BIGQUERY_DATASET"] = f"cm360_transformed__{brand}"
    return env


def set_env_vars_hivestack(report_id, brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"hivestack_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"hivestack_transformed__{brand}"
    env["HIVESTACK_REPORT_ID"] = report_id
    env["TAP_HIVESTACK_REPORT_ID"] = report_id
    # Do not set REPORT_NAME here — dbt parses all brand models and a shared
    # REPORT_NAME would make waitoa resolve to inghams_report (and vice versa).
    # Prod uses each model's SQL default (waitoa_report / inghams_report);
    # staging meltano sets REPORT_NAME=amp_report (RealNZ pattern).
    return env


def set_env_vars_cm360(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"cm360_transformed__{brand}"
    return env


def set_env_vars_tiktok(advertiser_id, brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"tiktok_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"tiktok_transformed__{brand}"
    env["TAP_TIKTOK_ADVERTISER_ID"] = advertiser_id
    return env


def set_env_vars_dash(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"dash_table__{brand}"
    return env


def set_env_vars_ga4(property_id, brand, goal):
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
    env["DBT_BIGQUERY_DATASET"] = f"ga4_transformed__{brand}"
    developer_creds = Credentials(
        None,
        refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
        client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
    )
    developer_creds.refresh(Request())
    env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
    env["TAP_GA4_PROPERTY_ID"] = property_id
    env["TAP_GA4_START_DATE"] = get_ga4_start_date()
    return env


# ==========================================================================
# DAG 1: Platform extract + transform → dash → dash_union
# ==========================================================================
with models.DAG(
    dag_id="inghams-meltano-extraction-transformation-dbt",
    schedule_interval="0 6 * * *",
    default_args=default_args,
) as dag:
    env = get_meltano_env()
    facebook_ids = {
        env["FACEBOOK_WAITOA_ID"]: "waitoa",
        env["FACEBOOK_INGHAMS_ID"]: "inghams",
    }
    dv360_ids = {
        env["DV360_WAITOA_ADVERTISER_ID"]: "waitoa",
        env["DV360_INGHAMS_ADVERTISER_ID"]: "inghams",
    }
    ttd_ids = {
        env["TTD_WAITOA_ID"]: "waitoa",
        env["TTD_INGHAMS_ID"]: "inghams",
    }
    hive_ids = {
        env["HIVESTACK_WAITOA_ID"]: "waitoa",
        env["HIVESTACK_INGHAMS_ID"]: "inghams",
    }

    brand_platform_tasks = {brand: [] for brand in BRANDS}

    for account_id, brand in facebook_ids.items():
        kube_facebook = KubernetesPodOperator(
            name=f"inghams-{brand}-facebook-to-bigquery",
            task_id=f"inghams-{brand}-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "run",
                "tap-facebook",
                "target-bigquery",
                f"dbt-bigquery:facebook_{brand}_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(account_id, brand),
            get_logs=True,
        )

        def make_facebook_comparison(b):
            def facebook_comparison_check(**context):
                meltano_env = get_meltano_env()
                trigger = ComparisonTrigger(
                    project_name=PROJECT_NAME,
                    destination_table=f"facebook_transformed__{b}",
                    table_name=f"facebook__{b}",
                    source_name="meta",
                    start_date=comparison_start_date,
                    end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                    secret_name=COMPARISON_SECRET,
                    project_id=meltano_env["PROJECT_ID"],
                    brand=b,
                )
                result = trigger.compare_data()
                if not result:
                    raise ValueError(f"Facebook data accuracy check failed for {b}.")
                return result

            return facebook_comparison_check

        task_facebook_comparison = PythonOperator(
            task_id=f"task_facebook_comparison_{brand}",
            python_callable=make_facebook_comparison(brand),
            retries=0,
            trigger_rule="all_done",
        )
        kube_facebook >> task_facebook_comparison
        brand_platform_tasks[brand].append(kube_facebook)

    for report_id, brand in hive_ids.items():
        kube_hivestack = KubernetesPodOperator(
            name=f"{brand}-hivestack-to-bigquery",
            task_id=f"{brand}-hivestack_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "run",
                "tap-hivestack",
                "target-bigquery",
                f"dbt-bigquery:hivestack_{brand}_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_hivestack(report_id, brand),
            get_logs=True,
        )
        brand_platform_tasks[brand].append(kube_hivestack)

    # CM360 must finish before DV360/TTD (both packages join cm360_direct_buy).
    cm360_by_brand = {}
    for brand in BRANDS:
        kube_cm360 = KubernetesPodOperator(
            name=f"{brand}-cm360-transformation",
            task_id=f"{brand}-cm360_transformation",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                f"dbt-bigquery:cm360_{brand}_models",
            ],
            env_vars=set_env_vars_cm360(brand),
            get_logs=True,
        )
        cm360_by_brand[brand] = kube_cm360
        brand_platform_tasks[brand].append(kube_cm360)

    for advertiser_id, brand in dv360_ids.items():
        kube_dv360 = KubernetesPodOperator(
            name=f"{brand}-dv360-to-bigquery",
            task_id=f"{brand}-dv360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "run",
                "tap-dv360",
                "target-bigquery",
                f"dbt-bigquery:dv360_{brand}_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dv360(advertiser_id, brand),
            get_logs=True,
        )

        def make_dv360_comparisons(b):
            def dv360_standard_comparison_check(**context):
                meltano_env = get_meltano_env()
                trigger = ComparisonTrigger(
                    project_name=PROJECT_NAME,
                    destination_table=f"dv360_transformed__{b}",
                    table_name=f"dv360_standard__{b}",
                    source_name="dv360_standard",
                    start_date=comparison_start_date,
                    end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                    secret_name=COMPARISON_SECRET,
                    project_id=meltano_env["PROJECT_ID"],
                    brand=b,
                )
                result = trigger.compare_data()
                if not result:
                    raise ValueError(
                        f"DV360 standard data accuracy check failed for {b}."
                    )
                return result

            def dv360_youtube_comparison_check(**context):
                meltano_env = get_meltano_env()
                trigger = ComparisonTrigger(
                    project_name=PROJECT_NAME,
                    destination_table=f"dv360_transformed__{b}",
                    table_name=f"dv360_youtube__{b}",
                    source_name="dv360_youtube",
                    start_date=comparison_start_date,
                    end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                    secret_name=COMPARISON_SECRET,
                    project_id=meltano_env["PROJECT_ID"],
                    brand=b,
                )
                result = trigger.compare_data()
                if not result:
                    raise ValueError(
                        f"DV360 YouTube data accuracy check failed for {b}."
                    )
                return result

            return dv360_standard_comparison_check, dv360_youtube_comparison_check

        dv360_std_fn, dv360_yt_fn = make_dv360_comparisons(brand)
        task_dv360_standard_comparison = PythonOperator(
            task_id=f"task_dv360_standard_comparison_{brand}",
            python_callable=dv360_std_fn,
            retries=0,
            trigger_rule="all_done",
        )
        task_dv360_youtube_comparison = PythonOperator(
            task_id=f"task_dv360_youtube_comparison_{brand}",
            python_callable=dv360_yt_fn,
            retries=0,
            trigger_rule="all_done",
        )
        cm360_by_brand[brand] >> kube_dv360
        kube_dv360 >> [
            task_dv360_standard_comparison,
            task_dv360_youtube_comparison,
        ]
        brand_platform_tasks[brand].append(kube_dv360)

    for advertiser_id, brand in ttd_ids.items():
        kube_ttd = KubernetesPodOperator(
            name=f"{brand}-ttd-to-bigquery",
            task_id=f"{brand}-ttd_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "run",
                "tap-ttd",
                "target-bigquery",
                f"dbt-bigquery:ttd_{brand}_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ttd(advertiser_id, brand),
            get_logs=True,
            execution_timeout=timedelta(minutes=60),
        )
        cm360_by_brand[brand] >> kube_ttd
        brand_platform_tasks[brand].append(kube_ttd)

    for brand in BRANDS:
        kube_dash = KubernetesPodOperator(
            name=f"{brand}-dash-to-bigquery",
            task_id=f"{brand}-dash_to_bigquery",
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
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )
        kube_dash_union = KubernetesPodOperator(
            name=f"{brand}-dash-union-to-bigquery",
            task_id=f"{brand}-dash_union_to_bigquery",
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
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )
        brand_platform_tasks[brand] >> kube_dash >> kube_dash_union


# ==========================================================================
# DAG 2: TikTok (waitoa) + GA4 → rebuild dash → dash_union → GA4 transforms
# ==========================================================================
with models.DAG(
    dag_id="inghams-meltano-google-ads",
    schedule_interval="10 14 * * *",
    default_args=default_args,
) as dag_afternoon:
    env = get_meltano_env()
    ga4_properties = {
        env["TAP_GA4_PROPERTY_ID_WAITOA"]: "waitoa",
        env["TAP_GA4_PROPERTY_ID_INGHAMS"]: "inghams",
    }
    tiktok_id = env["TIKTOK_WAITOA_ADVERTISER_ID"]

    kube_tiktok = KubernetesPodOperator(
        name="waitoa-tiktok-to-bigquery",
        task_id="waitoa-tiktok_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-tiktok",
            "target-bigquery",
            "--full-refresh",
            "dbt-bigquery:tiktok_waitoa_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_tiktok(tiktok_id, "waitoa"),
        get_logs=True,
    )

    def tiktok_comparison_check(**context):
        meltano_env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="tiktok_transformed__waitoa",
            table_name="tiktok__waitoa",
            source_name="tiktok",
            start_date=comparison_start_date,
            end_date=(datetime.datetime.now(local_tz) - timedelta(days=1)).strftime("%Y-%m-%d"),
            secret_name=COMPARISON_SECRET,
            project_id=meltano_env["PROJECT_ID"],
            brand="waitoa",
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("TikTok data accuracy check failed for waitoa.")
        return result

    task_tiktok_comparison = PythonOperator(
        task_id="task_tiktok_comparison_waitoa",
        python_callable=tiktok_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    kube_tiktok >> task_tiktok_comparison

    for brand in BRANDS:
        kube_dash = KubernetesPodOperator(
            name=f"{brand}-dash-to-bigquery-afternoon",
            task_id=f"{brand}-dash_to_bigquery_afternoon",
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
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )
        kube_dash_union = KubernetesPodOperator(
            name=f"{brand}-dash-union-to-bigquery-afternoon",
            task_id=f"{brand}-dash_union_to_bigquery_afternoon",
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
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        if brand == "waitoa":
            kube_tiktok >> kube_dash
        kube_dash >> kube_dash_union

        for property_id, ga4_brand in ga4_properties.items():
            if ga4_brand != brand:
                continue
            for goal in ["goal", "session", "keyword"]:
                kube_ga4 = KubernetesPodOperator(
                    name=f"{brand}-ga4-{goal}-to-bigquery",
                    task_id=f"{brand}-ga4_{goal}_to_bigquery",
                    namespace="composer-user-workloads",
                    image=IMAGE,
                    arguments=[
                        "--environment=prod",
                        "run",
                        "tap-ga4",
                        "target-bigquery",
                        f"dbt-bigquery:ga4_{goal}_{brand}_models",
                    ],
                    container_resources=k8s_models.V1ResourceRequirements(
                        limits={"memory": "1000M", "cpu": "500m"},
                    ),
                    env_vars=set_env_vars_ga4(property_id, brand, goal),
                    get_logs=True,
                )
                kube_dash_union >> kube_ga4
