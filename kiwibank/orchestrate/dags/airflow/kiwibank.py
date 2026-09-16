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
from comparison_package import ComparisonTrigger
from airflow.operators.python import PythonOperator
from datetime import timedelta
from google.cloud import storage
import json


client = storage.Client(project = "together-internal")
bucket = client.bucket("datapipeline-nonretryable-error")
blob = bucket.blob("non_retryable_error.json")


IMAGE = "australia-southeast1-docker.pkg.dev/kiwibank-main/meltano/meltano-kiwibank-main:prod"

log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")

comparison_start_date = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": datetime.datetime(2026, 4, 23, tzinfo=local_tz)
}



def get_meltano_env():
    # Generated function here, create your own secret, the first one stroes the specific ad account id,
    # second one is the common environment variables for all the projects, may need to refresh tiktok access token
    # third one is the environment variables for the GA4 project
    meltano_env_unique = Variable.get("meltano_kiwibank_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main", deserialize_json=True)
    meltano_env_ga4 = Variable.get("meltano_developer_ga4_main", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique, **meltano_env_ga4}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
    start_date_str = yesterday.strftime("%Y-%m-%d")
#
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

def set_env_vars_gpt():
    env = get_meltano_env()
    env["BQ_DATASET"] = "gpt_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = 'gpt_transformed'
    env["TAP_GPT_API_TOKEN"]=env["TAP_GPT_API_TOKEN"]
    env["TAP_GPT_START_DATE"]=env["START_DATE"]
    return env

def set_env_vars_ttd():
    env = get_meltano_env()
    env["BQ_DATASET"] = "ttd_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
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
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = f'google_ads_search_transformed__{brand}'
    return env

def set_env_vars_ga4_overall(goal):
    env = get_meltano_env()
    if goal == 'session':
        env["GA4_REPORTS"] = "./report_sessions.json"
        env["GA4_GOAL"] = 'session_goal'
    elif goal == 'keyword':
        env["GA4_REPORTS"] = "./report_keyword.json"
        env["GA4_GOAL"] = 'keyword_goal'
    else:
        env["GA4_REPORTS"] = "./report.json"
        env["GA4_GOAL"] = 'goal'
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
def set_env_vars_ga4_final():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = 'ga4_transformed'
    return env
def set_env_vars_ga4_brand(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed__{brand}'
    return env

def set_env_vars_dash(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = f'dash_table__{brand}'
    return env
def set_env_vars_dash_overall():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = 'dash_table'
    return env

def set_env_vars_dash_search(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{brand}'
    return env

def set_env_vars_cm360():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'kiwibank-main'
    env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
    return env


# ---------------------------------------------------------------------------
# DAG 1: Social / Display / Programmatic (schedule: 05:00 NZST daily)
# Extractors run once (single account), then per-brand dash tables
# Flow: [facebook, linkedin, dv360, hivestack, tiktok] >> per-brand [google_ads >> dash >> dash_search >> dash_union]
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="kiwibank-social-display-programmatic",
    schedule_interval="0 1 * * *",
    default_args=default_args
) as dag_social:

    kube_gpt = KubernetesPodOperator(
        name="kb-gpt-to-bq",
        task_id="kb-gpt_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-gpt", "target-bigquery",
                    "dbt-bigquery:gpt_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_gpt(),
        get_logs=True
    )
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
    def facebook_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name="kiwibank-main",
            destination_table="facebook_transformed",
            table_name="facebook",
            source_name="meta",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_kiwibank_main",
            project_id=env["PROJECT_ID"]
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("Facebook data accuracy check failed — BQ data does not match source API.")
        return result
    
    task_facebook_comparison = PythonOperator(
        task_id="task_facebook_comparison",
        python_callable=facebook_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    
    kube_facebook >> task_facebook_comparison
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

    def linkedin_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name="kiwibank-main",
            destination_table="linkedin_transformed",
            table_name="linkedin",
            source_name="linkedin",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_kiwibank_main",
            project_id=env["PROJECT_ID"]
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("Linkedin data accuracy check failed — BQ data does not match source API.")
        return result

    task_linkedin_comparison = PythonOperator(
        task_id="task_linkedin_comparison",
        python_callable=linkedin_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    def dv360_comparison_standard_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name="kiwibank-main",
            destination_table="dv360_transformed",
            table_name="dv360_standard",
            source_name="dv360_standard",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_kiwibank_main",
            project_id=env["PROJECT_ID"]
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("DV360 data accuracy check failed — BQ data does not match source API.")
        return result
    
    task_dv360_comparison_standard = PythonOperator(
        task_id="task_dv360_comparison_standard",
        python_callable=dv360_comparison_standard_check,
        retries=0,
        trigger_rule="all_done",
    )
    def dv360_comparison_youtube_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name="kiwibank-main",
            destination_table="dv360_transformed",
            table_name="dv360_youtube",
            source_name="dv360_youtube",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_kiwibank_main",
            project_id=env["PROJECT_ID"]
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("DV360 data accuracy check failed — BQ data does not match source API.")
        return result
    
    task_dv360_comparison_youtube = PythonOperator(
        task_id="task_dv360_comparison_youtube",
        python_callable=dv360_comparison_youtube_check,
        retries=0,
        trigger_rule="all_done",
    )

    
    kube_linkedin >> task_linkedin_comparison
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
    kube_dv360 >>[task_dv360_comparison_standard,task_dv360_comparison_youtube]

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

    kube_ttd = KubernetesPodOperator(
        name="kb-ttd-to-bq",
        task_id="kb-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery", "dbt-bigquery:ttd_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ttd(),
        get_logs=True
    )
   

    kube_dash_overall = KubernetesPodOperator(
        name="kb-dash-to-bq",
        task_id="kb-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_overall(),
        get_logs=True
    )
    kube_cm360 = KubernetesPodOperator(
        name="kb-cm360-to-bq",
        task_id="kb-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360(),
        get_logs=True
    )
    kube_cm360 >> kube_dv360 
    [kube_facebook,kube_linkedin,kube_dv360,kube_hivestack,kube_ttd,kube_gpt] >> kube_dash_overall
    task_list = []
    brands = [
        'everyday_banking_retail_deposit',
        'credit_card',
        'fraud_and_scams',
        'everyday_banking_join_kiwibank',
        'business_banking',
        'customer_business',
        'ao_social_boosting',
        'home_loans',
        'generic',
        "unattributed",
    ]
    social_only_brands = [
        "generic",
        "ao_social_boosting",
        "customer_business",
        "unattributed",
    ]

    for brand in brands:
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

        if brand in social_only_brands:
            kube_dash_overall >> kube_dash >> kube_dash_union
        else:
            kube_dash_overall >> kube_dash >> kube_dash_search >> kube_dash_union


# ---------------------------------------------------------------------------
# DAG 2: GA4 (schedule: 14:00 NZST daily)
# Flow: ga4 extraction + unified models >> per-brand ga4_goal_channel splits
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="kiwibank-ga4",
    schedule_interval="0 14 * * *",
    default_args=default_args
) as dag_ga4:
    ga4_task_list = []
    goal_list = ['goal','session','keyword']
    for goal in goal_list:

        kube_ga4_overall = KubernetesPodOperator(
            name=f"kb-ga4-to-bq_{goal}",
            task_id=f"kb-ga4_to_bigquery_{goal}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery",
                        f"dbt-bigquery:ga4_{goal}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4_overall(goal),
        )
        ga4_task_list.append(kube_ga4_overall)
    kube_ga4_final = KubernetesPodOperator(
        name="kb-ga4-final-to-bq",
        task_id="kb-ga4_final_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:ga4_final_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4_final(),
    )
    for task in ga4_task_list:
        task >> kube_ga4_final
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
    kube_dash_overall = KubernetesPodOperator(
        name="kb-dash-to-bq",
        task_id="kb-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_overall(),
        get_logs=True
    )
    kube_tiktok >> kube_dash_overall

    brands = [
        'everyday_banking_retail_deposit',
        'credit_card',
        'fraud_and_scams',
        'everyday_banking_join_kiwibank',
        'business_banking',
        'customer_business',
        'ao_social_boosting',
        'home_loans',
        'generic',
        'unattributed',
    ]
    social_only_brands = [
        "generic",
        "ao_social_boosting",
        "customer_business",
        "unattributed",
    ]
    bing_task_list = []
    for brand in brands:
        kube_ga4_brand = KubernetesPodOperator(
            name=f"kb-{brand}-ga4-channel-to-bq",
            task_id=f"kb-{brand}-ga4_goal_channel_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke",
                        f"dbt-bigquery:ga4_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4_brand(brand),
        )
        kube_ga4_keyword = KubernetesPodOperator(
            name=f"kb-{brand}-ga4-keyword-to-bq",
            task_id=f"kb-{brand}-ga4_keyword_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:ga4_keyword_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4_brand(brand),
        )
 
        kube_dash_overall>> kube_ga4_final >> [kube_ga4_brand, kube_ga4_keyword]


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
        bing_brands = [
            "home_loans",
            "everyday_banking_retail_deposit"
            
        ]
        if brand in bing_brands:
            kube_bing_ads = KubernetesPodOperator(
                name=f"kb-{brand}-bing-ads-to-bq",
                task_id=f"kb-{brand}-bing_ads_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "invoke", f"dbt-bigquery:bing_ads_{brand}_models"],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_google_ads_search(brand),
            )
            bing_task_list.append(kube_bing_ads)

            [kube_google_ads,kube_tiktok,kube_bing_ads] >> kube_dash >> kube_dash_search >> kube_dash_union >> kube_ga4_final >> kube_ga4_brand
        elif brand in social_only_brands:
            kube_tiktok >> kube_dash >> kube_dash_union >> kube_ga4_final >> kube_ga4_brand
        else:
            [kube_google_ads,kube_tiktok] >> kube_dash >> kube_dash_search >> kube_dash_union >> kube_ga4_final >> kube_ga4_brand

