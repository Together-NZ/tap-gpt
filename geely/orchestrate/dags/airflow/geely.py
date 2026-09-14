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

IMAGE = "australia-southeast1-docker.pkg.dev/geely-main/meltano/meltano-geely-main:prod"
PROJECT_NAME = "geely-main"
COMPARISON_SECRET = "airflow-variables-meltano_geely_main"
BRANDS = ["geely", "lotus"]

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
    "retry_delay": timedelta(minutes=30),
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz),
}

KUBE_RESOURCES = k8s_models.V1ResourceRequirements(
    limits={"memory": "1000M", "cpu": "500m"},
)


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_geely_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main", deserialize_json=True)
    meltano_env_ga4 = Variable.get("meltano_analytics_ga4_main", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique, **meltano_env_ga4}
    meltano_env["START_DATE"] = (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=29)
    ).strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
def set_env_vars_gpt(brand):
    env=get_meltano_env()
    env["BQ_DATASET"] = f"gpt_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"gpt_transformed__{brand}"
    report_key = f"TAP_GPT_{brand}_API_TOKEN"
    if report_key in env:
        env["TAP_GPTD_API_TOKEN"] = env[report_key]
    return env 

# ---------------------------------------------------------------------------
# DAG 1: Social / Display / Programmatic
# geely: facebook, linkedin, cm360 >> ttd >> dash >> dash_union
# lotus: linkedin, hivestack, cm360 >> ttd >> dash >> dash_union
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="geely-meltano-extraction-transformation-dbt",
    schedule_interval="0 5 * * *",
    default_args=default_args,
) as dag:

    def set_env_vars_ttd(brand):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"ttd_raw__{brand}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"ttd_transformed__{brand}"
        # Peer pattern (RealNZ/Warehouse): TAP_TTD_ADVERTISER_{brand}_ID → TAP_TTD_ADVERTISER_ID
        advertiser_key = f"TAP_TTD_ADVERTISER_{brand}_ID"
        if advertiser_key in env:
            env["TAP_TTD_ADVERTISER_ID"] = env[advertiser_key]
        env["TAP_TTD_START_DATE"] = get_ttd_start_date()

        return env

    def set_env_vars_hivestack(brand):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"hivestack_raw__{brand}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"hivestack_transformed__{brand}"
        report_key = f"TAP_HIVESTACK_REPORT_{brand}_ID"
        if report_key in env:
            env["TAP_HIVESTACK_REPORT_ID"] = env[report_key]
        return env

    def set_env_vars_facebook(brand):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"facebook_raw__{brand}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"facebook_transformed__{brand}"
        # Peer pattern: TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_{brand}_ID → ACCOUNT_ID
        account_key = f"TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_{brand}_ID"
        if account_key in env:
            env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"] = env[account_key]
            env["TAP_FACEBOOK_ACCOUNT_ID"] = env[account_key]
        return env

    def set_env_vars_linkedin(brand):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"linkedin_raw__{brand}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"linkedin_transformed__{brand}"
        # Peer pattern: TAP_LINKEDIN_ADS_ACCOUNTS_{brand}_ID → TAP_LINKEDIN_ADS_ACCOUNTS
        accounts_key = f"TAP_LINKEDIN_ADS_ACCOUNT_{brand}_ID"
        if accounts_key in env:
            env["TAP_LINKEDIN_ADS_ACCOUNTS"] = env[accounts_key]
        return env

    def set_env_vars_cm360(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"cm360_transformed__{brand}"
        return env

    def set_env_vars_dash(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"dash_table__{brand}"
        return env



    for brand in BRANDS:
        upstreams = []

        kube_cm360 = KubernetesPodOperator(
            name=f"geely-{brand}-cm360-to-bigquery",
            task_id=f"geely-{brand}-cm360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:cm360_{brand}_models"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_cm360(brand),
            get_logs=True,
        )
        upstreams.append(kube_cm360)

        kube_ttd = KubernetesPodOperator(
            name=f"geely-{brand}-ttd-to-bigquery",
            task_id=f"geely-{brand}-ttd_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery", f"dbt-bigquery:ttd_{brand}_models"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_ttd(brand),
            get_logs=True,
            execution_timeout=timedelta(minutes=120),
        )
        kube_cm360 >> kube_ttd
        upstreams.append(kube_ttd)

        kube_linkedin = KubernetesPodOperator(
            name=f"geely-{brand}-linkedin-to-bigquery",
            task_id=f"geely-{brand}-linkedin_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-linkedin-ads", "target-bigquery", f"dbt-bigquery:linkedin_{brand}_models"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_linkedin(brand),
            get_logs=True,
        )
        upstreams.append(kube_linkedin)

        def make_linkedin_comparison_check(brand_name):
            def linkedin_comparison_check(**context):
                env = get_meltano_env()
                trigger = ComparisonTrigger(
                    project_name=PROJECT_NAME,
                    destination_table=f"linkedin_transformed__{brand_name}",
                    table_name=f"linkedin__{brand_name}",
                    source_name="linkedin",
                    start_date=comparison_start_date,
                    end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                    secret_name=COMPARISON_SECRET,
                    project_id=env["PROJECT_ID"],
                    brand=brand_name,
                )
                result = trigger.compare_data()
                if not result:
                    raise ValueError(
                        f"LinkedIn data accuracy check failed for {brand_name} — "
                        "BQ data does not match source API."
                    )
                return result

            return linkedin_comparison_check

        task_linkedin_comparison = PythonOperator(
            task_id=f"task_linkedin_comparison_{brand}",
            python_callable=make_linkedin_comparison_check(brand),
            retries=0,
            trigger_rule="all_done",
        )
        kube_linkedin >> task_linkedin_comparison

        if brand == "geely":
            kube_facebook = KubernetesPodOperator(
                name=f"geely-{brand}-facebook-to-bigquery",
                task_id=f"geely-{brand}-facebook_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery", f"dbt-bigquery:facebook_{brand}_models"],
                container_resources=KUBE_RESOURCES,
                env_vars=set_env_vars_facebook(brand),
                get_logs=True,
            )
            upstreams.append(kube_facebook)

            def make_facebook_comparison_check(brand_name):
                def facebook_comparison_check(**context):
                    env = get_meltano_env()
                    trigger = ComparisonTrigger(
                        project_name=PROJECT_NAME,
                        destination_table=f"facebook_transformed__{brand_name}",
                        table_name=f"facebook__{brand_name}",
                        source_name="meta",
                        start_date=comparison_start_date,
                        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                        secret_name=COMPARISON_SECRET,
                        project_id=env["PROJECT_ID"],
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

            kube_gpt = KubernetesPodOperator(
                name="geely-gpt-to-bigquery",
                task_id="geely_gpt_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=[
                    "--environment=prod",
                    "run",
                    "tap-gpt",
                    "target-bigquery",
                    "dbt-bigquery:gpt_models",
                ],
                container_resources=KUBE_RESOURCES,
                env_vars=set_env_vars_gpt(brand),
                get_logs=True,
            )
            upstreams.append(kube_gpt)

        if brand == "lotus":
            kube_hivestack = KubernetesPodOperator(
                name=f"geely-{brand}-hivestack-to-bigquery",
                task_id=f"geely-{brand}-hivestack_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery", f"dbt-bigquery:hivestack_{brand}_models"],
                container_resources=KUBE_RESOURCES,
                env_vars=set_env_vars_hivestack(brand),
                get_logs=True,
            )
            upstreams.append(kube_hivestack)

        kube_dash = KubernetesPodOperator(
            name=f"geely-{brand}-dash-to-bigquery",
            task_id=f"geely-{brand}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule="all_done",
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"geely-{brand}-dash-union-to-bigquery",
            task_id=f"geely-{brand}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        upstreams >> kube_dash >> kube_dash_union


# ---------------------------------------------------------------------------
# DAG 2: Google Ads + GA4
# geely: google_ads >> [dash, dash_search] >> dash_union >> ga4 >> final
# lotus: dash >> dash_union >> ga4 >> final (no Google Ads)
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="geely-meltano-google_ads",
    schedule_interval="0 14 * * *",
    default_args=default_args,
) as google_dag:

    def set_env_vars_google_ads_search(brand):
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

    def set_env_vars_dash_search(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"dash_table_search__{brand}"
        return env

    def set_env_vars_ga4_final(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"ga4_transformed__{brand}"
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
        env["GA4_REPORTS"] = env["TAP_GA4_REPORTS"]
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
        env["START_DATE"] = get_ga4_start_date()
        env["TAP_GA4_START_DATE"] = get_ga4_start_date()
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        # Peer pattern: TAP_GA4_PROPERTY_{brand}_ID → TAP_GA4_PROPERTY_ID
        property_key = f"TAP_GA4_PROPERTY_{brand}_ID"
        if property_key in env:
            env["TAP_GA4_PROPERTY_ID"] = env[property_key]
        return env

    for brand in BRANDS:
        kube_dash = KubernetesPodOperator(
            name=f"geely-{brand}-dash-to-bigquery",
            task_id=f"geely-{brand}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule="all_done",
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"geely-{brand}-dash-union-to-bigquery",
            task_id=f"geely-{brand}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        kube_ga4_final = KubernetesPodOperator(
            name=f"geely-{brand}-ga4-final-to-bigquery",
            task_id=f"geely-{brand}-ga4_final_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:ga4_{brand}_final_models"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_ga4_final(brand),
            get_logs=True,
        )

        kube_ga4_list = []
        for goal in ["goal", "session", "keyword"]:
            kube_ga4_t = KubernetesPodOperator(
                name=f"geely-{brand}-ga4-{goal}-to-bigquery",
                task_id=f"geely-{brand}-ga4_{goal}_to_bigquery",
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
            kube_ga4_list.append(kube_ga4_t)

        if brand == "geely":
            kube_google_ads_search = KubernetesPodOperator(
                name=f"geely-{brand}-google-ads-search-to-bigquery",
                task_id=f"geely-{brand}-google-ads-search_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "invoke", f"dbt-bigquery:google_ads_{brand}_models"],
                container_resources=KUBE_RESOURCES,
                env_vars=set_env_vars_google_ads_search(brand),
                get_logs=True,
            )

            kube_dash_search = KubernetesPodOperator(
                name=f"geely-{brand}-dash-search-to-bigquery",
                task_id=f"geely-{brand}-dash_search_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table_search__{brand}"],
                container_resources=KUBE_RESOURCES,
                env_vars=set_env_vars_dash_search(brand),
                get_logs=True,
            )

            kube_google_ads_search >> [kube_dash, kube_dash_search]
            [kube_dash, kube_dash_search] >> kube_dash_union
        else:
            kube_dash >> kube_dash_union

        for task in kube_ga4_list:
            kube_dash_union >> task >> kube_ga4_final
