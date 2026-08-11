import datetime
from airflow import models
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
import pendulum
from kubernetes.client import models as k8s_models
from copy import deepcopy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
import sys
import logging
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import json
import time
from datetime import timedelta,datetime, timezone
import datetime
from google.cloud import secretmanager
from google.cloud import storage 
from google.cloud import storage
import json
from comparison_package import ComparisonTrigger


IMAGE = "australia-southeast1-docker.pkg.dev/curative-main/meltano/meltano-curative-main:prod"
PROJECT_NAME = "curative-main"
COMPARISON_SECRET = "airflow-variables-meltano_curative_main"
# Per-brand source API secrets use comparison_package taxonomy:
# TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_{brand}_ID, TAP_DV360_ADVERTISER_{brand}_ID,
# TAP_SNAPCHAT_ADS_AD_ACCOUNT_{brand}_ID, TAP_TIKTOK_ADVERTISER_{brand}_ID, etc.
FACEBOOK_BRANDS = ["creativenz", "fasd", "protectyourbreath"]
DV360_BRANDS = ["fasd", "mahi", "protectyourbreath", "creativenz"]
SNAPCHAT_BRANDS = ["protectyourbreath"]
TIKTOK_BRANDS = ["protectyourbreath", "creativenz", "mahi"]
TTD_BRANDS = ["creativenz", "fasd", "mahi", "realnurses", "protectyourbreath"]
HIVESTACK_BRANDS = ["protectyourbreath", "fasd"]
GA4_BRANDS = ["fasd", "realnurses", "mahi"]

comparison_start_date = (
    datetime.datetime.now(pendulum.timezone("Pacific/Auckland"))
    - datetime.timedelta(days=30)
).strftime("%Y-%m-%d")


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)
all_tasks = []
per_label_task = {}
tiktok_task_list = {}
local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=13)
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    'retry_delay': datetime.timedelta(minutes=30),
    "start_date": datetime.datetime(2025, 12, 20, tzinfo=local_tz)
}

def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_curative_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=13)
    start_date_str = yesterday.strftime("%Y-%m-%d")

    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)
def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
with models.DAG(
    dag_id="curative-meltano-google-ads",
    schedule_interval="44 14 * * *",
    default_args=default_args,
) as google_dag:
    env = get_meltano_env()
    def set_env_vars_google_ads_search(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'google_ads_search_transformed__{label}'
        return env
    def set_env_vars_dash_table_search(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{label}'
        return env
    def set_env_vars_dash(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__{label}'
        return env
    def set_env_vars_tiktok(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"tiktok_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'tiktok_transformed__{label}'
        advertiser_key = f"TAP_TIKTOK_ADVERTISER_{label}_ID"
        if advertiser_key in env:
            env["TAP_TIKTOK_ADVERTISER_ID"] = env[advertiser_key]
        return env
    
    def set_env_vars_ga4(label, goal="goal"):
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
        env["BQ_DATASET"] = f"ga4_raw__{label}"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed__{label}'
        property_key = f"TAP_GA4_PROPERTY_{label}_ID"
        if property_key in env:
            env["TAP_GA4_PROPERTY_ID"] = env[property_key]
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

    def set_env_vars_ga4_final(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed__{label}'
        return env

    for label in TIKTOK_BRANDS:
        kube_tiktok = KubernetesPodOperator(
            name=f"curative-{label}-tiktok-to-bigquery",
            task_id=f"curative-{label}-tiktok_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-tiktok", "target-bigquery",f"dbt-bigquery:tiktok_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_tiktok(label),
            get_logs=True,
        )
        all_tasks.append(kube_tiktok)
        tiktok_task_list.setdefault(label, []).append(kube_tiktok)

        def make_tiktok_comparison(b):
            def tiktok_comparison_check(**context):
                meltano_env = get_meltano_env()
                trigger = ComparisonTrigger(
                    project_name=PROJECT_NAME,
                    destination_table=f"tiktok_transformed__{b}",
                    table_name=f"tiktok__{b}",
                    source_name="tiktok",
                    start_date=comparison_start_date,
                    end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                    secret_name=COMPARISON_SECRET,
                    project_id=meltano_env["PROJECT_ID"],
                    brand=b,
                )
                result = trigger.compare_data()
                if not result:
                    raise ValueError(f"TikTok data accuracy check failed for {b}.")
                return result
            return tiktok_comparison_check

        task_tiktok_comparison = PythonOperator(
            task_id=f"task_tiktok_comparison_{label}",
            python_callable=make_tiktok_comparison(label),
            retries=0,
            trigger_rule="all_done",
        )
        kube_tiktok >> task_tiktok_comparison
    ga4_label_task = {}
    ga4_final_task = {}
    for label in GA4_BRANDS:
        ga4_tasks = []
        for goal_type in ("goal", "session", "keyword"):
            kube_ga4 = KubernetesPodOperator(
                name=f"curative-{label}-ga4-{goal_type}-to-bigquery",
                task_id=f"curative-{label}-ga4_{goal_type}_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=[
                    "--environment=prod",
                    "run",
                    "tap-ga4",
                    "target-bigquery",
                    f"dbt-bigquery:ga4_{label}_{goal_type}_models",
                ],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_ga4(label, goal_type),
                get_logs=True,
            )
            all_tasks.append(kube_ga4)
            ga4_tasks.append(kube_ga4)

        kube_ga4_final = KubernetesPodOperator(
            name=f"curative-{label}-ga4-final-to-bigquery",
            task_id=f"curative-{label}-ga4_final_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:ga4_{label}_final_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4_final(label),
            get_logs=True,
        )
        all_tasks.append(kube_ga4_final)
        ga4_label_task[label] = ga4_tasks
        ga4_final_task[label] = kube_ga4_final

    all_labels=['mahi','realnurses','protectyourbreath','creativenz','fasd']
    dash_list={}
    dash_union_list={}
    for label in all_labels:
        
            
        kube_dash_union = KubernetesPodOperator(
            name=f"curative-{label}-dash-union-to-bigquery",
            task_id=f"curative-{label}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(label),
            )
        
        kube_dash = KubernetesPodOperator(
            name=f"curative-{label}-dash-to-bigquery",
            task_id=f"curative-{label}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            trigger_rule = 'all_done',
            env_vars=set_env_vars_dash(label),
            #base_container_name=f"meltano-{label}-dash",
        )
        dash_list.setdefault(label, []).append(kube_dash)
        dash_union_list.setdefault(label, []).append(kube_dash_union)
        

        kube_google_ads_search  = KubernetesPodOperator(
            name = f'curative-{label}-google-ads-search-to-bigquery',
            task_id = f'curative-{label}-google-ads-search_to_bigquery',
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:google_ads_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(label),
            #base_container_name=f"meltano-{label}-google-ads-search",
        )
        kube_dash_table_search = KubernetesPodOperator(
            name = f'curative-{label}-dash-table-search-to-bigquery',
            task_id = f'curative-{label}-dash-table-search_to_bigquery',
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table_search__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_table_search(label),
            #base_container_name=f"meltano-{label}-dash-table-search",
        )
        kube_google_ads_search >> kube_dash>> kube_dash_table_search >> kube_dash_union 
       
    tiktok_labels=['creativenz','mahi','protectyourbreath']
    for label in tiktok_labels:
        for tiktok in tiktok_task_list[label]:
            for dash in dash_list[label]:
                tiktok >> dash
    for label in GA4_BRANDS:
        for dash_union in dash_union_list[label]:
            dash_union >> ga4_label_task[label] >> ga4_final_task[label]

    # hnz runs TikTok only, so it gets dash_table -> dash_union with no search leg.
    if env.get("TAP_TIKTOK_ADVERTISER_hnz_ID"):
        kube_tiktok_hnz = KubernetesPodOperator(
            name="curative-hnz-tiktok-to-bigquery",
            task_id="curative-hnz-tiktok_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-tiktok", "target-bigquery","dbt-bigquery:tiktok_hnz_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_tiktok('hnz'),
            get_logs=True,
        )
        all_tasks.append(kube_tiktok_hnz)

        kube_dash_hnz = KubernetesPodOperator(
            name="curative-hnz-dash-to-bigquery",
            task_id="curative-hnz-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_table__hnz"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            trigger_rule = 'all_done',
            env_vars=set_env_vars_dash('hnz'),
        )

        kube_dash_union_hnz = KubernetesPodOperator(
            name="curative-hnz-dash-union-to-bigquery",
            task_id="curative-hnz-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_union__hnz"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash('hnz'),
        )

        kube_tiktok_hnz >> kube_dash_hnz >> kube_dash_union_hnz

        
with models.DAG(
    dag_id="curative-meltano-extraction-transformation-dbt",
    schedule_interval="0 1 * * *",
    default_args=default_args,
) as dag:
    env = get_meltano_env()

        
    def set_env_vars_hivestack(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"hivestack_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'hivestack_transformed__{label}'
        report_key = f"TAP_HIVESTACK_REPORT_{label}_ID"
        if report_key in env:
            env["TAP_HIVESTACK_REPORT_ID"] = env[report_key]
        return env

    def set_env_vars_snapchat(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"snapchat_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'snapchat_transformed__{label}'
        account_key = f"TAP_SNAPCHAT_ADS_AD_ACCOUNT_{label}_ID"
        if account_key in env:
            env["TAP_SNAPCHAT_ADS_AD_ACCOUNT_IDS"] = env[account_key]
        return env

    def set_env_vars_facebook(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"facebook_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'facebook_transformed__{label}'
        account_key = f"TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_{label}_ID"
        if account_key in env:
            env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"] = env[account_key]
            env["TAP_FACEBOOK_ACCOUNT_ID"] = env[account_key]
        return env
    def set_env_vars_cm360(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'cm360_transformed__{label}'
        return env
    def set_env_vars_dv360(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"dv360_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'dv360_transformed__{label}'
        advertiser_key = f"TAP_DV360_ADVERTISER_{label}_ID"
        if advertiser_key in env:
            env["DV360_ADVERTISER_ID"] = env[advertiser_key]
            env["TAP_DV360_ADVERTISER_ID"] = env[advertiser_key]
        return env

    def set_env_vars_ttd(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"ttd_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'ttd_transformed__{label}'
        advertiser_key = f"TAP_TTD_ADVERTISER_{label}_ID"
        if advertiser_key in env:
            env["TAP_TTD_ADVERTISER_ID"] = env[advertiser_key]
        env["TAP_TTD_START_DATE"] = get_ttd_start_date()
        return env
    def set_env_vars_dash_table_search(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{label}'
        return env
    def set_env_vars_dash(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__{label}'
        return env

    env = get_meltano_env()
    for label in SNAPCHAT_BRANDS:
        kube_snapchat = KubernetesPodOperator(
            name=f"curative-{label}-snapchat-to-bigquery",
            task_id=f"curative-{label}-snapchat_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-snapchat-ads", "target-bigquery", f"dbt-bigquery:snapchat_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_snapchat(label),
            get_logs=True,
        )
        all_tasks.append(kube_snapchat)
        per_label_task.setdefault(label, []).append(kube_snapchat)

        def make_snapchat_comparison(b):
            def snapchat_comparison_check(**context):
                meltano_env = get_meltano_env()
                trigger = ComparisonTrigger(
                    project_name=PROJECT_NAME,
                    destination_table=f"snapchat_transformed__{b}",
                    table_name=f"snapchat__{b}",
                    source_name="snapchat",
                    start_date=comparison_start_date,
                    end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                    secret_name=COMPARISON_SECRET,
                    project_id=meltano_env["PROJECT_ID"],
                    brand=b,
                )
                result = trigger.compare_data()
                if not result:
                    raise ValueError(f"Snapchat data accuracy check failed for {b}.")
                return result
            return snapchat_comparison_check

        task_snapchat_comparison = PythonOperator(
            task_id=f"task_snapchat_comparison_{label}",
            python_callable=make_snapchat_comparison(label),
            retries=0,
            trigger_rule="all_done",
        )
        kube_snapchat >> task_snapchat_comparison
        per_label_task.setdefault(label, []).append(task_snapchat_comparison)
    for label in HIVESTACK_BRANDS:
        kube_hivestack = KubernetesPodOperator(
            name=f"curative-{label}-hivestack-to-bigquery",
            task_id=f"curative-{label}-hivestack_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery",f"dbt-bigquery:hivestack_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_hivestack(label),
            get_logs=True,
        )
        all_tasks.append(kube_hivestack)
        per_label_task.setdefault(label, []).append(kube_hivestack)


    cm360_tasks = {}
    # CM360 feeds both TTD and DV360 package joins — run for every brand that has either.
    cm360_brands = set(TTD_BRANDS) | set(DV360_BRANDS)
    for label in sorted(cm360_brands):
        kube_cm360 = KubernetesPodOperator(
            name=f"curative-{label}-cm360-transformation",
            task_id=f"curative-{label}-cm360_transformation",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:cm360_{label}_models"],
            env_vars=set_env_vars_cm360(label),
            get_logs=True,
        )
        all_tasks.append(kube_cm360)
        per_label_task.setdefault(label, []).append(kube_cm360)
        cm360_tasks[label] = kube_cm360

    for label in TTD_BRANDS:
        kube_ttd = KubernetesPodOperator(
            name=f"curative-{label}-ttd-to-bigquery",
            task_id=f"curative-{label}-ttd_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery",f"dbt-bigquery:ttd_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ttd(label),
            get_logs=True,
            execution_timeout=timedelta(minutes=60),
        )
        all_tasks.append(kube_ttd)
        per_label_task.setdefault(label, []).append(kube_ttd)
        if label in cm360_tasks:
            cm360_tasks[label] >> kube_ttd
    for label in FACEBOOK_BRANDS:
        kube_facebook = KubernetesPodOperator(
            name=f"curative-{label}-facebook-to-bigquery",
            task_id=f"curative-{label}-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery",f"dbt-bigquery:facebook_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(label),
            get_logs=True,
        )
        all_tasks.append(kube_facebook)
        per_label_task.setdefault(label, []).append(kube_facebook)

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
            task_id=f"task_facebook_comparison_{label}",
            python_callable=make_facebook_comparison(label),
            retries=0,
            trigger_rule="all_done",
        )
        kube_facebook >> task_facebook_comparison
        per_label_task.setdefault(label, []).append(task_facebook_comparison)
    for label in DV360_BRANDS:
        kube_dv360 = KubernetesPodOperator(
            name=f"curative-{label}-dv360-to-bigquery",
            task_id=f"curative-{label}-dv360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery",f"dbt-bigquery:dv360_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dv360(label),
            get_logs=True,
        )
        all_tasks.append(kube_dv360)
        per_label_task.setdefault(label, []).append(kube_dv360)
        if label in cm360_tasks:
            cm360_tasks[label] >> kube_dv360

        def make_dv360_comparison(b, stream):
            def dv360_comparison_check(**context):
                meltano_env = get_meltano_env()
                trigger = ComparisonTrigger(
                    project_name=PROJECT_NAME,
                    destination_table=f"dv360_transformed__{b}",
                    table_name=f"dv360_{stream}__{b}",
                    source_name=f"dv360_{stream}",
                    start_date=comparison_start_date,
                    end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
                    secret_name=COMPARISON_SECRET,
                    project_id=meltano_env["PROJECT_ID"],
                    brand=b,
                )
                result = trigger.compare_data()
                if not result:
                    raise ValueError(
                        f"DV360 {stream} data accuracy check failed for {b}."
                    )
                return result
            return dv360_comparison_check

        for stream in ("standard", "youtube"):
            task_dv360_comparison = PythonOperator(
                task_id=f"task_dv360_{stream}_comparison_{label}",
                python_callable=make_dv360_comparison(label, stream),
                retries=0,
                trigger_rule="all_done",
            )
            kube_dv360 >> task_dv360_comparison
            per_label_task.setdefault(label, []).append(task_dv360_comparison)

    all_labels = set(FACEBOOK_BRANDS) | set(DV360_BRANDS) | set(TTD_BRANDS) | set(HIVESTACK_BRANDS) | set(SNAPCHAT_BRANDS)
    for label in all_labels:
        kube_dash_union = KubernetesPodOperator(
            name=f"curative-{label}-dash-union-to-bigquery",
            task_id=f"curative-{label}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(label),
            )
        
        kube_dash = KubernetesPodOperator(
            name=f"curative-{label}-dash-to-bigquery",
            task_id=f"curative-{label}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            trigger_rule = 'all_done',
            env_vars=set_env_vars_dash(label),
            #base_container_name=f"meltano-{label}-dash",
        )
        for upstream_task in per_label_task.get(label,[]):
            upstream_task >> kube_dash

        kube_dash_table_search = KubernetesPodOperator(
            name = f'curative-{label}-dash-table-search-to-bigquery',
            task_id = f'curative-{label}-dash-table-search_to_bigquery',
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table_search__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_table_search(label),
            #base_container_name=f"meltano-{label}-dash-table-search",
        )

        kube_dash >> kube_dash_table_search >> kube_dash_union


    