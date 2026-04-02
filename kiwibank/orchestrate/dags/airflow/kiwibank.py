import datetime
from airflow import models
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.models import Variable
import pendulum
from kubernetes.client import models as k8s_models
from copy import deepcopy
import logging
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import timedelta
from google.cloud import storage


IMAGE = "australia-southeast1-docker.pkg.dev/kiwibank-main/meltano/meltano-kiwibank-main:prod"

log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz)
}


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_kiwibank_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main", deserialize_json=True)
    meltano_env_ga4 = Variable.get("meltano_analytics_ga4_main", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique, **meltano_env_ga4}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
    start_date_str = yesterday.strftime("%Y-%m-%d")

    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)


def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def set_env_vars_facebook():
    env = get_meltano_env()
    env["BQ_DATASET"] = "facebook_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
    return env


def set_env_vars_linkedin():
    env = get_meltano_env()
    env["BQ_DATASET"] = "linkedin_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = 'linkedin_transformed'
    return env


def set_env_vars_dv360():
    env = get_meltano_env()
    env["BQ_DATASET"] = "dv360_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = 'dv360_transformed'
    return env


def set_env_vars_hivestack():
    env = get_meltano_env()
    env["BQ_DATASET"] = "hivestack_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = 'hivestack_transformed'
    return env


def set_env_vars_tiktok():
    env = get_meltano_env()
    env["BQ_DATASET"] = "tiktok_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = 'tiktok_transformed'
    return env


def set_env_vars_google_ads_search(brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = "google_ads_search"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = f'google_ads_search_transformed__{brand}'
    return env


def set_env_vars_ga4(brand='kiwibank'):
    env = get_meltano_env()
    env["BQ_DATASET"] = "ga4_raw"
    env["BQ_METHOD"] = "gcs_stage"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed'
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
    env["TAP_GA4_PROPERTY_ID"] = env.get('TAP_GA4_PROPERTY_ID', '')
    return env


def set_env_vars_dash(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = f'dash_table__{brand}'
    return env


def set_env_vars_dash_search(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{brand}'
    return env


# ---------------------------------------------------------------------------
# DAG 1: Social / Display / Programmatic (schedule: 05:00 NZST daily)
# Extractors run once (single account), then per-brand dash tables
# Flow: [facebook, linkedin, dv360, hivestack, tiktok] >> per-brand [google_ads >> dash >> dash_search >> dash_union]
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="kiwibank-social-display-programmatic",
    schedule_interval="0 5 * * *",
    default_args=default_args
) as dag_social:

    kube_facebook = KubernetesPodOperator(
        name="kb-facebook-to-bq",
        task_id="kb-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery",
                    "dbt-bigquery:facebook_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_facebook(),
        get_logs=True
    )

    kube_linkedin = KubernetesPodOperator(
        name="kb-linkedin-to-bq",
        task_id="kb-linkedin_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-linkedin-ads", "target-bigquery",
                    "dbt-bigquery:linkedin_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_linkedin(),
        get_logs=True
    )

    kube_dv360 = KubernetesPodOperator(
        name="kb-dv360-to-bq",
        task_id="kb-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery",
                    "dbt-bigquery:dv360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360(),
    )

    kube_hivestack = KubernetesPodOperator(
        name="kb-hivestack-to-bq",
        task_id="kb-hivestack_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery",
                    "dbt-bigquery:hivestack_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_hivestack(),
        get_logs=True
    )

    kube_tiktok = KubernetesPodOperator(
        name="kb-tiktok-to-bq",
        task_id="kb-tiktok_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-tiktok", "target-bigquery",
                    "dbt-bigquery:tiktok_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_tiktok(),
    )

    brands = [
        'everyday_banking_retail_deposit',
        'credit_card',
        'fraud_and_scams',
        'everyday_banking_join_kiwibank',
        'business_banking',
        'ao_social_boosting',
        'home_loan',
    ]

    for brand in brands:
        kube_google_ads = KubernetesPodOperator(
            name=f"kb-{brand}-google-ads-to-bq",
            task_id=f"kb-{brand}-google_ads_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:google_ads_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(brand),
            get_logs=True
        )

        kube_dash = KubernetesPodOperator(
            name=f"kb-{brand}-dash-to-bq",
            task_id=f"kb-{brand}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
        )

        kube_dash_search = KubernetesPodOperator(
            name=f"kb-{brand}-dash-search-to-bq",
            task_id=f"kb-{brand}-dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table_search__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(brand),
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"kb-{brand}-dash-union-to-bq",
            task_id=f"kb-{brand}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
        )

        [kube_facebook, kube_linkedin, kube_dv360, kube_hivestack, kube_tiktok, kube_google_ads] >> kube_dash >> kube_dash_search >> kube_dash_union


# ---------------------------------------------------------------------------
# DAG 2: GA4 (schedule: 14:00 NZST daily)
# Flow: ga4 extraction + unified models >> per-brand ga4_goal_channel splits
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="kiwibank-ga4",
    schedule_interval="0 14 * * *",
    default_args=default_args
) as dag_ga4:

    kube_ga4 = KubernetesPodOperator(
        name="kb-ga4-to-bq",
        task_id="kb-ga4_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery",
                    "dbt-bigquery:ga4_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4('kiwibank'),
    )

    brands = [
        'everyday_banking_retail_deposit',
        'credit_card',
        'fraud_and_scams',
        'everyday_banking_join_kiwibank',
        'business_banking',
        'ao_social_boosting',
        'home_loan',
    ]

    for brand in brands:
        kube_ga4_brand = KubernetesPodOperator(
            name=f"kb-{brand}-ga4-channel-to-bq",
            task_id=f"kb-{brand}-ga4_goal_channel_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke",
                        f"dbt-bigquery:ga4_goal_channel_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(brand),
        )

        kube_ga4 >> kube_ga4_brand
