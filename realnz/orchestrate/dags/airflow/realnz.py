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

IMAGE = "australia-southeast1-docker.pkg.dev/real-nz-main/meltano/meltano-real-nz-main:prod"
PROJECT_NAME = "real-nz-main"
BRANDS = ["mountain", "tourism"]

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
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=14)).strftime("%Y-%m-%d")


def get_meta_start_date():
    return (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=3)
    ).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_realnz_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main", deserialize_json=True)
    meltano_env_ga4 = Variable.get("meltano_developer_ga4_main", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique, **meltano_env_ga4}
    meltano_env["START_DATE"] = (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=13)
    ).strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def set_env_vars_hivestack(report_id, brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"hivestack_raw__{brand}"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"hivestack_transformed__{brand}"
    env["TAP_HIVESTACK_REPORT_ID"] = report_id
    return env


def set_env_vars_ga4(property_id, brand, goal_type):
    env = get_meltano_env()
    if goal_type == "session":
        env["TAP_GA4_REPORTS"] = "./report_sessions.json"
        env["GA4_GOAL"] = "session_goal"
    elif goal_type == "keyword":
        env["TAP_GA4_REPORTS"] = "./report_keyword.json"
        env["GA4_GOAL"] = "keyword_goal"
    elif goal_type == "ecommerce":
        env["TAP_GA4_REPORTS"] = "./ecommerce_report.json"
        env["GA4_GOAL"] = "ecommerce_goal"
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


def set_env_vars_facebook(account_id, brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"facebook_raw__{brand}"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"facebook_transformed__{brand}"
    env["TAP_FACEBOOK_ACCOUNT_ID"] = account_id
    env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"] = account_id
    env["TAP_FACEBOOK_AIRBYTE_CONFIG_START_DATE"] = get_meta_start_date()
    return env


def set_env_vars_cm360(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"cm360_transformed__{brand}"
    return env


def set_env_vars_dv360(account_id, brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"dv360_raw__{brand}"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"dv360_transformed__{brand}"
    env["TAP_DV360_ADVERTISER_ID"] = account_id
    return env


def set_env_vars_ttd(advertiser_id, brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"ttd_raw__{brand}"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"ttd_transformed__{brand}"
    env["TAP_TTD_ADVERTISER_ID"] = advertiser_id
    env["TAP_TTD_START_DATE"] = get_ttd_start_date()
    return env


def set_env_vars_tiktok(advertiser_id, brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"tiktok_raw__{brand}"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"tiktok_transformed__{brand}"
    env["TAP_TIKTOK_ADVERTISER_ID"] = advertiser_id
    return env


def set_env_vars_google_ads_dv():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "google_ads_search_transformed__tourism"
    return env


def set_env_vars_google_ads_search(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"google_ads_search_transformed__{brand}"
    bing_key = f"BING_ADS_CLIENT_{brand}_ID"
    if bing_key in env:
        env["BING_ADS_CLIENT_ID"] = env[bing_key]
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


# ---------------------------------------------------------------------------
# DAG 1: Social / Display / Programmatic (schedule: 05:00 NZST daily)
# Flow: extractors >> per-brand dash >> dash_search >> dash_union
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="realnz-social-display-programmatic",
    schedule_interval="0 5 * * *",
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=120),
) as dag_social:
    env = get_meltano_env()
    per_brand_upstreams = {brand: [] for brand in BRANDS}

    for brand in BRANDS:
        kube_hivestack = KubernetesPodOperator(
            name=f"realnz-hivestack-to-bigquery-{brand}",
            task_id=f"realnz_hivestack_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery", f"dbt-bigquery:hivestack_{brand}_models"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_hivestack(env[f"TAP_HIVESTACK_REPORT_{brand}_ID"], brand),
        )
        per_brand_upstreams[brand].append(kube_hivestack)

        kube_facebook = KubernetesPodOperator(
            name=f"realnz-facebook-to-bigquery-{brand}",
            task_id=f"realnz_facebook_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery", f"dbt-bigquery:facebook_{brand}_models"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_facebook(env[f"TAP_FACEBOOK_ACCOUNT_{brand}_ID"], brand),
        )
        per_brand_upstreams[brand].append(kube_facebook)

        def make_facebook_comparison_check(brand_name):
            def facebook_comparison_check(**context):
                meltano_env = get_meltano_env()
                trigger = ComparisonTrigger(
                    project_name=PROJECT_NAME,
                    destination_table=f"facebook_transformed__{brand_name}",
                    table_name=f"facebook__{brand_name}",
                    source_name="meta",
                    start_date=comparison_start_date,
                    end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                    secret_name="airflow-variables-meltano_realnz_main",
                    project_id=meltano_env["PROJECT_ID"],
                    brand=brand_name,
                )
                result = trigger.compare_data()
                if not result:
                    raise ValueError(
                        f"Facebook data accuracy check failed for {brand_name} — "
                        "BQ data does not match source API."
                    )
                return result

            return facebook_comparison_check

        task_facebook_comparison = PythonOperator(
            task_id=f"task_facebook_comparison_{brand}",
            python_callable=make_facebook_comparison_check(brand),
            retries=0,
            trigger_rule="all_done",
        )
        kube_facebook >> task_facebook_comparison

        kube_cm360 = KubernetesPodOperator(
            name=f"realnz-cm360-to-bigquery-{brand}",
            task_id=f"realnz_cm360_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:cm360_{brand}_models"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_cm360(brand),
        )
        per_brand_upstreams[brand].append(kube_cm360)

        kube_dv360 = KubernetesPodOperator(
            name=f"realnz-dv360-to-bigquery-{brand}",
            task_id=f"realnz_dv360_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery", f"dbt-bigquery:dv360_{brand}_models"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dv360(env[f"TAP_DV360_ADVERTISER_{brand}_ID"], brand),
        )
        per_brand_upstreams[brand].append(kube_dv360)

        def make_dv360_standard_comparison_check(brand_name):
            def dv360_standard_comparison_check(**context):
                meltano_env = get_meltano_env()
                trigger = ComparisonTrigger(
                    project_name=PROJECT_NAME,
                    destination_table=f"dv360_transformed__{brand_name}",
                    table_name=f"dv360_standard__{brand_name}",
                    source_name="dv360_standard",
                    start_date=comparison_start_date,
                    end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                    secret_name="airflow-variables-meltano_realnz_main",
                    project_id=meltano_env["PROJECT_ID"],
                    brand=brand_name,
                )
                result = trigger.compare_data()
                if not result:
                    raise ValueError(
                        f"DV360 standard data accuracy check failed for {brand_name} — "
                        "BQ data does not match source API."
                    )
                return result

            return dv360_standard_comparison_check

        def make_dv360_youtube_comparison_check(brand_name):
            def dv360_youtube_comparison_check(**context):
                meltano_env = get_meltano_env()
                trigger = ComparisonTrigger(
                    project_name=PROJECT_NAME,
                    destination_table=f"dv360_transformed__{brand_name}",
                    table_name=f"dv360_youtube__{brand_name}",
                    source_name="dv360_youtube",
                    start_date=comparison_start_date,
                    end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                    secret_name="airflow-variables-meltano_realnz_main",
                    project_id=meltano_env["PROJECT_ID"],
                    brand=brand_name,
                )
                result = trigger.compare_data()
                if not result:
                    raise ValueError(
                        f"DV360 YouTube data accuracy check failed for {brand_name} — "
                        "BQ data does not match source API."
                    )
                return result

            return dv360_youtube_comparison_check

        task_dv360_standard_comparison = PythonOperator(
            task_id=f"task_dv360_standard_comparison_{brand}",
            python_callable=make_dv360_standard_comparison_check(brand),
            retries=0,
            trigger_rule="all_done",
        )
        task_dv360_youtube_comparison = PythonOperator(
            task_id=f"task_dv360_youtube_comparison_{brand}",
            python_callable=make_dv360_youtube_comparison_check(brand),
            retries=0,
            trigger_rule="all_done",
        )
        kube_dv360 >> [task_dv360_standard_comparison, task_dv360_youtube_comparison]

        kube_ttd = KubernetesPodOperator(
            name=f"realnz-ttd-to-bigquery-{brand}",
            task_id=f"realnz_ttd_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery", f"dbt-bigquery:ttd_{brand}_models"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_ttd(env[f"TAP_TTD_ADVERTISER_{brand}_ID"], brand),
            execution_timeout=timedelta(minutes=60),
        )
        per_brand_upstreams[brand].append(kube_ttd)

        # DV360 / TTD join CM360 direct_buy (need video_* + dv360_* columns from package schema)
        kube_cm360 >> kube_dv360
        kube_cm360 >> kube_ttd

    for brand in BRANDS:
        kube_dash = KubernetesPodOperator(
            name=f"realnz-dash-to-bigquery-{brand}",
            task_id=f"realnz_dash_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule="all_done",
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
        )
        kube_dash_search = KubernetesPodOperator(
            name=f"realnz-dash-search-to-bigquery-{brand}",
            task_id=f"realnz_dash_search_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"+dash_table_search__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash_search(brand),
        )
        kube_dash_union = KubernetesPodOperator(
            name=f"realnz-dash-union-to-bigquery-{brand}",
            task_id=f"realnz_dash_union_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
        )
        for upstream in per_brand_upstreams[brand]:
            upstream >> kube_dash
        kube_dash >> kube_dash_search >> kube_dash_union


# ---------------------------------------------------------------------------
# DAG 2: Google Ads + TikTok + GA4 (schedule: 14:00 NZST daily)
# Flow: [google_ads (+ bing/dv), tiktok] >> dash >> dash_search >> dash_union >> ga4
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="realnz-google-ads-ga4",
    schedule_interval="0 14 * * *",
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=120),
) as dag_google:
    env = get_meltano_env()

    kube_google_ads_dv = KubernetesPodOperator(
        name="realnz-google-ads-dv-to-bigquery",
        task_id="realnz_google_ads_dv_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:google_ads_dv_models__realnz"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_google_ads_dv(),
    )

    kube_tiktok = KubernetesPodOperator(
        name="realnz-tiktok-to-bigquery-mountain",
        task_id="realnz_tiktok_to_bigquery_mountain",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-tiktok",
            "target-bigquery",
            "--full-refresh",
            "dbt-bigquery:tiktok_mountain_models",
        ],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_tiktok(env["TAP_TIKTOK_ADVERTISER_mountain_ID"], "mountain"),
    )

    def tiktok_comparison_check(**context):
        meltano_env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="tiktok_transformed__mountain",
            table_name="tiktok__mountain",
            source_name="tiktok",
            start_date=comparison_start_date,
            end_date=(datetime.datetime.now(local_tz) - timedelta(days=1)).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_realnz_main",
            project_id=meltano_env["PROJECT_ID"],
            brand="mountain",
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("TikTok data accuracy check failed — BQ data does not match source API.")
        return result

    task_tiktok_comparison = PythonOperator(
        task_id="task_tiktok_comparison",
        python_callable=tiktok_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    kube_tiktok >> task_tiktok_comparison

    for brand in BRANDS:
        kube_google_ads = KubernetesPodOperator(
            name=f"realnz-google-ads-to-bigquery-{brand}",
            task_id=f"realnz_google_ads_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:google_ads_{brand}_models"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_google_ads_search(brand),
            get_logs=True,
        )

        kube_dash = KubernetesPodOperator(
            name=f"realnz-google-dash-to-bigquery-{brand}",
            task_id=f"realnz_google_dash_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule="all_done",
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
        )

        kube_dash_search = KubernetesPodOperator(
            name=f"realnz-google-dash-search-to-bigquery-{brand}",
            task_id=f"realnz_google_dash_search_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"+dash_table_search__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash_search(brand),
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"realnz-google-dash-union-to-bigquery-{brand}",
            task_id=f"realnz_google_dash_union_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
        )

        ga4_tasks = []
        for goal_type in ["goal", "ecommerce", "session", "keyword"]:
            property_id = env[f"TAP_GA4_PROPERTY_{brand}_ID"]
            kube_ga4 = KubernetesPodOperator(
                name=f"realnz-ga4-{brand}-{goal_type}-to-bigquery",
                task_id=f"realnz_ga4_to_bigquery_{brand}_{goal_type}",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=[
                    "--environment=prod",
                    "run",
                    "tap-ga4",
                    "target-bigquery",
                    f"dbt-bigquery:ga4_{brand}_{goal_type}_models",
                ],
                container_resources=KUBE_RESOURCES,
                env_vars=set_env_vars_ga4(property_id, brand, goal_type),
                get_logs=True,
            )
            ga4_tasks.append(kube_ga4)

        dash_upstreams = [kube_google_ads, kube_tiktok]
        if brand == "tourism":
            dash_upstreams.append(kube_google_ads_dv)
        dash_upstreams >> kube_dash
        kube_dash >> kube_dash_search >> kube_dash_union
        for ga4_task in ga4_tasks:
            kube_dash_union >> ga4_task
