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


IMAGE = "australia-southeast1-docker.pkg.dev/kiwirail-main/meltano/meltano-kiwirail-main:prod"
PROJECT_NAME = "kiwirail-main"
COMPARISON_SECRET = "airflow-variables-meltano_kiwirail_main"

# Tourism: Facebook for both. DV360/CM360 only for great_journey + freight.
TOURISM_BRANDS = ["interislander", "great_journey"]
DV360_BRANDS = ["great_journey", "freight"]
FREIGHT_BRAND = "freight"
ALL_BRANDS = TOURISM_BRANDS + [FREIGHT_BRAND]

DEFAULT_DV360_ADVERTISER_ID = "8048813517"

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
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2026, 3, 24, tzinfo=local_tz),
}


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_kiwirail_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main", deserialize_json=True)
    meltano_env_ga4 = Variable.get("meltano_developer_ga4_main", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique, **meltano_env_ga4}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=29)
    meltano_env["START_DATE"] = yesterday.strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def get_facebook_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime(
        "%Y-%m-%dT00:00:00Z"
    )


def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime(
        "%Y-%m-%d"
    )


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
    env["TAP_GA4_PROPERTY_ID"] = env[f"TAP_GA4_PROPERTY_ID_{brand.upper()}"]
    env["TAP_GA4_START_DATE"] = get_ga4_start_date()
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


def set_env_vars_facebook(brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"facebook_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"facebook_transformed__{brand}"
    env["TAP_FACEBOOK_AIRBYTE_CONFIG_START_DATE"] = get_facebook_start_date()
    # Prefer comparison_package key shape; fall back to legacy brand key.
    account_key = f"TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_{brand}_ID"
    legacy_key = f"TAP_FACEBOOK_{brand.upper()}_AIRBYTE_CONFIG_ACCOUNT_ID"
    env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"] = env.get(
        account_key, env[legacy_key]
    )
    return env


def set_env_vars_cm360(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"cm360_transformed__{brand}"
    env["CM360_SUFFIX"] = f"cm360_transformed__{brand}"
    env["REFERENCE_CM360_TRANSFORMED_BIGQUERY_DATASET"] = f"cm360_transformed__{brand}"
    return env


def set_env_vars_dv360(brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"dv360_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"dv360_transformed__{brand}"
    env["CM360_SUFFIX"] = f"cm360_transformed__{brand}"
    env["REFERENCE_CM360_TRANSFORMED_BIGQUERY_DATASET"] = f"cm360_transformed__{brand}"
    # comparison_package / Warehouse shape: TAP_DV360_ADVERTISER_{brand}_ID
    advertiser_key = f"TAP_DV360_ADVERTISER_{brand}_ID"
    env["TAP_DV360_ADVERTISER_ID"] = env.get(
        advertiser_key,
        env.get(
            f"TAP_DV360_ADVERTISER_ID_{brand.upper()}",
            env.get("TAP_DV360_ADVERTISER_ID", DEFAULT_DV360_ADVERTISER_ID),
        ),
    )
    return env


def set_env_vars_linkedin_freight():
    env = get_meltano_env()
    env["BQ_DATASET"] = "linkedin_raw__freight"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "linkedin_transformed__freight"
    env["TAP_LINKEDIN_ADS_ACCOUNTS"] = env.get(
        "TAP_LINKEDIN_ADS_ACCOUNTS_FREIGHT",
        env.get(
            "TAP_LINKEDIN_ADS_ACCOUNT_freight_ID",
            env.get("TAP_LINKEDIN_ADS_ACCOUNTS", ""),
        ),
    )
    return env


def set_env_vars_ttd_freight():
    env = get_meltano_env()
    env["BQ_DATASET"] = "ttd_raw__freight"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "ttd_transformed__freight"
    env["CM360_SUFFIX"] = "cm360_transformed__freight"
    env["REFERENCE_CM360_TRANSFORMED_BIGQUERY_DATASET"] = "cm360_transformed__freight"
    env["TAP_TTD_ADVERTISER_ID"] = env.get("TAP_TTD_ADVERTISER_ID", "zdnpghz")
    return env


# ==========================================================================
# DAG 1: Google Ads + GA4
# ==========================================================================
with models.DAG(
    dag_id="kiwirail-meltano-google-ads",
    schedule_interval="0 14 * * *",
    default_args=default_args,
) as dag_google_ads:

    for brand in ALL_BRANDS:
        kube_google_ads = KubernetesPodOperator(
            name=f"kiwirail-google-ads-{brand}-to-bigquery",
            task_id=f"kiwirail-google_ads_{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                f"dbt-bigquery:google_ads_{brand}_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads(brand),
            get_logs=True,
        )

        ga4_tasks = []
        for goal in ["goal", "session", "keyword"]:
            kube_ga4 = KubernetesPodOperator(
                name=f"kiwirail-{brand}-ga4-{goal}-to-bigquery",
                task_id=f"kiwirail-{brand}-ga4_{goal}_to_bigquery",
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
                env_vars=set_env_vars_ga4(brand, goal),
                get_logs=True,
            )
            ga4_tasks.append(kube_ga4)

        kube_dash = KubernetesPodOperator(
            name=f"kiwirail-dash-{brand}-to-bigquery",
            task_id=f"kiwirail-dash_{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
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
            trigger_rule="all_done",
            get_logs=True,
        )

        kube_dash_search = KubernetesPodOperator(
            name=f"kiwirail-dash-search-{brand}-to-bigquery",
            task_id=f"kiwirail-dash_search_{brand}_to_bigquery",
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
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(brand),
            get_logs=True,
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"kiwirail-dash-union-{brand}-to-bigquery",
            task_id=f"kiwirail-dash_union_{brand}_to_bigquery",
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

        kube_google_ads >> kube_dash
        ga4_tasks >> kube_dash
        kube_dash >> kube_dash_search >> kube_dash_union


# ==========================================================================
# DAG 2: Main extraction + transformation
# ==========================================================================
with models.DAG(
    dag_id="kiwirail-meltano-extraction-transformation-dbt",
    schedule_interval="0 5 * * *",
    default_args=default_args,
) as dag:

    for brand in TOURISM_BRANDS:
        kube_facebook = KubernetesPodOperator(
            name=f"kiwirail-{brand}-facebook-to-bigquery",
            task_id=f"kiwirail-{brand}-facebook_to_bigquery",
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
            env_vars=set_env_vars_facebook(brand),
            get_logs=True,
        )

        def make_facebook_comparison(b):
            def facebook_comparison_check(**context):
                env = get_meltano_env()
                trigger = ComparisonTrigger(
                    project_name=PROJECT_NAME,
                    destination_table=f"facebook_transformed__{b}",
                    table_name=f"facebook__{b}",
                    source_name="meta",
                    start_date=comparison_start_date,
                    end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                    secret_name=COMPARISON_SECRET,
                    project_id=env["PROJECT_ID"],
                    brand=b,
                )
                result = trigger.compare_data()
                if not result:
                    raise ValueError(
                        f"Facebook data accuracy check failed for {b}."
                    )
                return result

            return facebook_comparison_check

        task_facebook_comparison = PythonOperator(
            task_id=f"task_facebook_comparison_{brand}",
            python_callable=make_facebook_comparison(brand),
            retries=0,
            trigger_rule="all_done",
        )

        dash_upstreams = [kube_facebook]
        if brand in DV360_BRANDS:
            kube_cm360 = KubernetesPodOperator(
                name=f"kiwirail-{brand}-cm360-to-bigquery",
                task_id=f"kiwirail-{brand}-cm360_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=[
                    "--environment=prod",
                    "invoke",
                    f"dbt-bigquery:cm360_{brand}_models",
                ],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_cm360(brand),
                get_logs=True,
            )

            kube_dv360 = KubernetesPodOperator(
                name=f"kiwirail-{brand}-dv360-to-bigquery",
                task_id=f"kiwirail-{brand}-dv360_to_bigquery",
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
                env_vars=set_env_vars_dv360(brand),
                get_logs=True,
            )

            def make_dv360_comparisons(b):
                def dv360_standard_comparison_check(**context):
                    env = get_meltano_env()
                    trigger = ComparisonTrigger(
                        project_name=PROJECT_NAME,
                        destination_table=f"dv360_transformed__{b}",
                        table_name=f"dv360_standard__{b}",
                        source_name="dv360_standard",
                        start_date=comparison_start_date,
                        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                        secret_name=COMPARISON_SECRET,
                        project_id=env["PROJECT_ID"],
                        brand=b,
                    )
                    result = trigger.compare_data()
                    if not result:
                        raise ValueError(
                            f"DV360 standard data accuracy check failed for {b}."
                        )
                    return result

                def dv360_youtube_comparison_check(**context):
                    env = get_meltano_env()
                    trigger = ComparisonTrigger(
                        project_name=PROJECT_NAME,
                        destination_table=f"dv360_transformed__{b}",
                        table_name=f"dv360_youtube__{b}",
                        source_name="dv360_youtube",
                        start_date=comparison_start_date,
                        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                        secret_name=COMPARISON_SECRET,
                        project_id=env["PROJECT_ID"],
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

            kube_cm360 >> kube_dv360
            kube_dv360 >> [
                task_dv360_standard_comparison,
                task_dv360_youtube_comparison,
            ]
            dash_upstreams.extend([kube_cm360, kube_dv360])

        kube_dash = KubernetesPodOperator(
            name=f"kiwirail-{brand}-dash-to-bigquery",
            task_id=f"kiwirail-{brand}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
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
            trigger_rule="all_done",
            get_logs=True,
        )

        kube_dash_search = KubernetesPodOperator(
            name=f"kiwirail-{brand}-dash-search-to-bigquery",
            task_id=f"kiwirail-{brand}-dash_search_to_bigquery",
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
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(brand),
            get_logs=True,
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"kiwirail-{brand}-dash-union-to-bigquery",
            task_id=f"kiwirail-{brand}-dash_union_to_bigquery",
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

        kube_facebook >> task_facebook_comparison
        dash_upstreams >> kube_dash >> kube_dash_search >> kube_dash_union

    # Freight: LinkedIn + TTD + DV360/CM360
    brand = FREIGHT_BRAND
    kube_linkedin = KubernetesPodOperator(
        name="kiwirail-freight-linkedin-to-bigquery",
        task_id="kiwirail-freight-linkedin_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-linkedin-ads",
            "target-bigquery",
            "dbt-bigquery:linkedin_freight_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_linkedin_freight(),
        get_logs=True,
    )

    def linkedin_freight_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="linkedin_transformed__freight",
            table_name="linkedin__freight",
            source_name="linkedin",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name=COMPARISON_SECRET,
            project_id=env["PROJECT_ID"],
            brand=FREIGHT_BRAND,
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("LinkedIn freight data accuracy check failed.")
        return result

    task_linkedin_comparison = PythonOperator(
        task_id="task_linkedin_comparison_freight",
        python_callable=linkedin_freight_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )

    kube_cm360_freight = KubernetesPodOperator(
        name="kiwirail-freight-cm360-to-bigquery",
        task_id="kiwirail-freight-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery:cm360_freight_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360(brand),
        get_logs=True,
    )

    kube_ttd = KubernetesPodOperator(
        name="kiwirail-freight-ttd-to-bigquery",
        task_id="kiwirail-freight-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-ttd",
            "target-bigquery",
            "dbt-bigquery:ttd_freight_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        execution_timeout=timedelta(minutes=60),
        env_vars=set_env_vars_ttd_freight(),
        get_logs=True,
    )

    kube_dv360_freight = KubernetesPodOperator(
        name="kiwirail-freight-dv360-to-bigquery",
        task_id="kiwirail-freight-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-dv360",
            "target-bigquery",
            "dbt-bigquery:dv360_freight_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360(brand),
        get_logs=True,
    )

    def dv360_freight_standard_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="dv360_transformed__freight",
            table_name="dv360_standard__freight",
            source_name="dv360_standard",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name=COMPARISON_SECRET,
            project_id=env["PROJECT_ID"],
            brand=FREIGHT_BRAND,
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("DV360 freight standard data accuracy check failed.")
        return result

    def dv360_freight_youtube_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table="dv360_transformed__freight",
            table_name="dv360_youtube__freight",
            source_name="dv360_youtube",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name=COMPARISON_SECRET,
            project_id=env["PROJECT_ID"],
            brand=FREIGHT_BRAND,
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("DV360 freight YouTube data accuracy check failed.")
        return result

    task_dv360_freight_standard_comparison = PythonOperator(
        task_id="task_dv360_standard_comparison_freight",
        python_callable=dv360_freight_standard_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    task_dv360_freight_youtube_comparison = PythonOperator(
        task_id="task_dv360_youtube_comparison_freight",
        python_callable=dv360_freight_youtube_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )

    kube_dash_freight = KubernetesPodOperator(
        name="kiwirail-freight-dash-to-bigquery",
        task_id="kiwirail-freight-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_table__freight",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(brand),
        trigger_rule="all_done",
        get_logs=True,
    )

    kube_dash_search_freight = KubernetesPodOperator(
        name="kiwirail-freight-dash-search-to-bigquery",
        task_id="kiwirail-freight-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_table_search__freight",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search(brand),
        get_logs=True,
    )

    kube_dash_union_freight = KubernetesPodOperator(
        name="kiwirail-freight-dash-union-to-bigquery",
        task_id="kiwirail-freight-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_union__freight",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(brand),
        get_logs=True,
    )

    kube_linkedin >> task_linkedin_comparison
    kube_cm360_freight >> [kube_ttd, kube_dv360_freight]
    kube_dv360_freight >> [
        task_dv360_freight_standard_comparison,
        task_dv360_freight_youtube_comparison,
    ]
    [kube_linkedin, kube_ttd, kube_dv360_freight, kube_cm360_freight] >> kube_dash_freight
    kube_dash_freight >> kube_dash_search_freight >> kube_dash_union_freight
