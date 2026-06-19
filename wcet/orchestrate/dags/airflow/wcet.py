"""
Airflow DAG for wcet Meltano pipeline (Facebook + TikTok + DV360, Beervana).

See cupra/orchestrate/dags/airflow/cupra.py for full reference.
"""

import datetime
from copy import deepcopy
from datetime import timedelta
from google.oauth2.credentials import Credentials
import pendulum
from airflow import models
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s_models
from google.auth.transport.requests import Request
from comparison_package import ComparisonTrigger
from airflow.operators.python import PythonOperator

IMAGE = "australia-southeast1-docker.pkg.dev/wcet-main/meltano/meltano-wcet-main:prod"
PROJECT_NAME = "wcet-main"

local_tz = pendulum.timezone("Pacific/Auckland")

default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "retry_delay": timedelta(minutes=30),
    "start_date": datetime.datetime(2026, 5, 28, tzinfo=local_tz),
}
comparison_start_date = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_wcet_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main", deserialize_json=True)
    meltano_env_ga4 = Variable.get("meltano_developer_ga4_main", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique, **meltano_env_ga4}
    meltano_env["START_DATE"] = (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
    ).strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)
def set_env_vars_ga4_final(brand):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"ga4_transformed__{brand}"
    return env
def set_env_vars_facebook(brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"facebook_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"facebook_transformed__{brand}"
    env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"] = env[f"TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_{brand}_ID"]
    return env


def set_env_vars_tiktok(brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"tiktok_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"tiktok_transformed__{brand}"
    env["TAP_TIKTOK_ADVERTISER_ID"] = env[f"TAP_TIKTOK_ADVERTISER_{brand}_ID"]
    return env


def set_env_vars_dv360(brand):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"dv360_raw__{brand}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"dv360_transformed__{brand}"
    env["TAP_DV360_ADVERTISER_ID"] = env[f"TAP_DV360_ADVERTISER_{brand}_ID"]
    return env

def set_env_vars_ga4(brand,goal):
    env = get_meltano_env()
    #if goal == 'ecommerce':
    if goal == 'session':
            env["TAP_GA4_REPORTS"] = "./report_sessions.json"
            env["GA4_GOAL"] = 'session_goal'
    elif goal == 'keyword':
            env["TAP_GA4_REPORTS"] = "./report_keyword.json"
            env["GA4_GOAL"] = 'keyword_goal'
    else:
            env["TAP_GA4_REPORTS"] = "./report.json"
            env["GA4_GOAL"] = 'goal'   
    env["BQ_DATASET"] = f"ga4_raw__{brand}"
    env["BQ_METHOD"] = "gcs_stage"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_AUTH_METHOD"]='oauth'
    env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed__{brand}'       
    developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
    developer_creds.refresh(Request())
    env["TAP_GA4_START_DATE"]  = get_ga4_start_date()
    env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
    env["TAP_GA4_PROPERTY_ID"] = env[f"TAP_GA4_PROPERTY_{brand}_ID"]
    return env

def set_env_vars_google_ads(brand):
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

with models.DAG(
    dag_id="wcet-meltano-google-ads",
    schedule_interval="0 14 * * *",
    default_args=default_args,
    tags=["wcet", "meltano", "beervana"],
) as dag_google_ads:
    brands = ['beervana','wop']
    for brand in brands:
        kube_google_ads = KubernetesPodOperator(
            name="wcet-google-ads-to-bigquery",
            task_id=f"wcet-google-ads__{brand}_to_bigquery",
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
        kube_dash = KubernetesPodOperator(
            name="wcet-dash-to-bigquery",
            task_id=f"wcet-dash__{brand}_to_bigquery",
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
            trigger_rule="all_done",
            env_vars=set_env_vars_dash(brand),
            
            get_logs=True,
        )
        kube_dash_search = KubernetesPodOperator(
            name="wcet-dash-search-to-bigquery",
            task_id=f"wcet-dash_search__{brand}_to_bigquery",
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
            trigger_rule="all_done",
            env_vars=set_env_vars_dash_search(brand),
            get_logs=True,
        )
        kube_dash_union = KubernetesPodOperator(
            name="wcet-dash-union-to-bigquery",
            task_id=f"wcet-dash_union__{brand}_to_bigquery",
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
            trigger_rule="all_done",
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )
        kube_ga4_list = []
        ga4_type=['session','goal','keyword']
        for type in ga4_type:
            kube_ga4 = KubernetesPodOperator(
                name="wcet-ga4-to-bigquery",
                task_id=f"wcet-ga4__{brand}_{type}_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=[
                    "--environment=prod",
                    "run",
                    "tap-ga4",
                    "target-bigquery",
                    f"dbt-bigquery:ga4_{brand}_{type}_models",
                ],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_ga4(brand,type),
                get_logs=True,
            )
            kube_dash_union >> kube_ga4
            kube_ga4_list.append(kube_ga4)
        kube_ga4_final = KubernetesPodOperator(
            name="wcet-ga4-final-to-bigquery",
            task_id=f"wcet-ga4_final__{brand}_to_bigquery",
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
            env_vars=set_env_vars_ga4_final(brand),
            get_logs=True,)
        for task in kube_ga4_list:
            task >> kube_ga4_final
        kube_google_ads >> kube_dash >> kube_dash_search >> kube_dash_union
        
        
        
        
with models.DAG(
    dag_id="wcet-meltano-extraction-transformation-dbt",
    schedule_interval="0 4 * * *",
    default_args=default_args,
    tags=["wcet", "meltano", "beervana"],
) as dag:
    tiktok_task_list = []
    brands = ['beervana','wop']
    for brand in brands:
        kube_facebook = KubernetesPodOperator(
            name="wcet-facebook-to-bigquery",
            task_id=f"wcet-facebook__{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery",
                        f"dbt-bigquery:facebook_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(brand),
            get_logs=True
        )
        def facebook_comparison_check(**context):
            env = get_meltano_env()
            trigger = ComparisonTrigger(
                project_name="wcet-main",
                destination_table=f"facebook_transformed__{brand}",
                table_name=f"facebook__{brand}",
                source_name="meta",
                start_date=comparison_start_date,
                end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                secret_name="airflow-variables-meltano_wcet_main",
                project_id=env["PROJECT_ID"]
            )
            result = trigger.compare_data()
            if not result:
                raise ValueError("Facebook data accuracy check failed — BQ data does not match source API.")
            return result
        
        task_facebook_comparison = PythonOperator(
            task_id=f"wcet-facebook_comparison__{brand}",
            python_callable=facebook_comparison_check,
            retries=0,
            trigger_rule="all_done",
        )
        kube_facebook = KubernetesPodOperator(
            name="wcet-facebook-to-bigquery",
            task_id=f"wcet-facebook__{brand}_to_bigquery",
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
        kube_facebook >> task_facebook_comparison
        if brand == 'beervana':
            kube_tiktok = KubernetesPodOperator(
                name="wcet-tiktok-to-bigquery",
                task_id=f"wcet-tiktok__{brand}_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=[
                    "--environment=prod",
                    "run",
                    "tap-tiktok",
                    "target-bigquery",
                    f"dbt-bigquery:tiktok_{brand}_models",
                ],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_tiktok(brand),
                get_logs=True,
            )
            def tiktok_comparison_check(**context):
                env = get_meltano_env()
                trigger = ComparisonTrigger(
                    project_name="wcet-main",
                    destination_table=f"tiktok_transformed__{brand} ",
                    table_name=f"tiktok__{brand}",
                    source_name="tiktok",
                    start_date=comparison_start_date,
                    end_date=(datetime.datetime.now(local_tz) - timedelta(days=1)).strftime("%Y-%m-%d"),
                    secret_name="airflow-variables-meltano_wcet_main",
                    project_id=env["PROJECT_ID"]
                )
                result = trigger.compare_data()
                if not result:
                    raise ValueError("Tiktok data accuracy check failed — BQ data does not match source API.")
                return result
            task_tiktok_comparison = PythonOperator(
                task_id=f"wcet-tiktok_comparison__{brand}",
                python_callable=tiktok_comparison_check,
                retries=0,
                trigger_rule="all_done",
            )
            kube_tiktok >> task_tiktok_comparison
 
        kube_dash = KubernetesPodOperator(
            name="wcet-dash-to-bigquery",
            task_id=f"wcet-dash__{brand}_to_bigquery",
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
            trigger_rule="all_done",
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )
        kube_dash_search = KubernetesPodOperator(
            name="wcet-dash-search-to-bigquery",
            task_id=f"wcet-dash_search__{brand}_to_bigquery",
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
            trigger_rule="all_done",
            env_vars=set_env_vars_dash_search(brand),
            get_logs=True,
        )
        kube_dash_union = KubernetesPodOperator(
            name="wcet-dash-union-to-bigquery",
            task_id=f"wcet-dash_union__{brand}_to_bigquery",
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
            trigger_rule="all_done",
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        kube_dv360 = KubernetesPodOperator(
            name="wcet-dv360-to-bigquery",
            task_id=f"wcet-dv360__{brand}_to_bigquery",
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
        dv360_comparison_check = PythonOperator(
            task_id=f"wcet-dv360_comparison__{brand}",
            python_callable=dv360_comparison_check,
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
                project_name="wcet-main",
                destination_table=f"dv360_transformed__{brand}",
                table_name=f"dv360_youtube__{brand}",
                source_name="dv360_youtube",
                start_date=comparison_start_date,
                end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                secret_name="airflow-variables-meltano_wcet_main",
                project_id=env["PROJECT_ID"]
            )
            result = trigger.compare_data()
            if not result:
                raise ValueError("DV360 data accuracy check failed — BQ data does not match source API.")
            return result
        
        task_dv360_comparison_youtube = PythonOperator(
            task_id=f"wcet-dv360_comparison_youtube__{brand}",
            python_callable=dv360_comparison_youtube_check,
            retries=0,
            trigger_rule="all_done",
        )

        kube_dv360 >> [task_dv360_comparison_standard,task_dv360_comparison_youtube]
        if brand == 'beervana':
            for task in tiktok_task_list:
                [kube_facebook, task, kube_dv360] >> kube_dash >> kube_dash_search >> kube_dash_union
        else:
            [kube_facebook, kube_dv360] >> kube_dash >> kube_dash_search >> kube_dash_union
