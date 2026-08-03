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

IMAGE = "australia-southeast1-docker.pkg.dev/cffc-main/meltano/meltano-cffc-main:prod"
PROJECT_NAME = "cffc-main"
COMPARISON_SECRET = "airflow-variables-meltano_cffc_main"
PLATFORM_LABEL = "cffc"
DASH_BRAND_RETIREMENT = "retirement_commission"
GOOGLE_ADS_BRANDS = ["sorted", "sorted_in_school"]

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
    "start_date": datetime.datetime(2026, 7, 30, tzinfo=local_tz),
}

KUBE_RESOURCES = k8s_models.V1ResourceRequirements(
    limits={"memory": "1000M", "cpu": "500m"},
)


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_cffc_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=13)
    ).strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# DAG 1: Social / Display / Programmatic (__cffc platforms → retirement dash)
# Flow: CM360 >> [DV360, TTD]; platforms >> dash >> dash_union
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="cffc-meltano-extraction-transformation-dbt",
    schedule_interval="0 2 * * *",
    default_args=default_args,
) as dag:

    def set_env_vars_linkedin():
        env = get_meltano_env()
        env["BQ_DATASET"] = "linkedin_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"linkedin_transformed__{PLATFORM_LABEL}"
        return env

    def set_env_vars_snapchat():
        env = get_meltano_env()
        env["BQ_DATASET"] = "snapchat_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"snapchat_transformed__{PLATFORM_LABEL}"
        return env

    def set_env_vars_facebook():
        env = get_meltano_env()
        env["BQ_DATASET"] = f"facebook_raw__{PLATFORM_LABEL}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"facebook_transformed__{PLATFORM_LABEL}"
        return env

    def set_env_vars_cm360():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"cm360_transformed__{PLATFORM_LABEL}"
        return env

    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"dv360_transformed__{PLATFORM_LABEL}"
        return env

    def set_env_vars_ttd():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"ttd_transformed__{PLATFORM_LABEL}"
        env["TAP_TTD_START_DATE"] = get_ttd_start_date()
        return env

    def set_env_vars_hivestack():
        env = get_meltano_env()
        env["BQ_DATASET"] = "hivestack_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"hivestack_transformed__{PLATFORM_LABEL}"
        return env

    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"dash_table__{DASH_BRAND_RETIREMENT}"
        return env

    def facebook_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table=f"facebook_transformed__{PLATFORM_LABEL}",
            table_name=f"facebook__{PLATFORM_LABEL}",
            source_name="meta",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name=COMPARISON_SECRET,
            project_id=env["PROJECT_ID"],
            brand=PLATFORM_LABEL,
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("Facebook data accuracy check failed — BQ data does not match source API.")
        return result

    def linkedin_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table=f"linkedin_transformed__{PLATFORM_LABEL}",
            table_name=f"linkedin__{PLATFORM_LABEL}",
            source_name="linkedin",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name=COMPARISON_SECRET,
            project_id=env["PROJECT_ID"],
            brand=PLATFORM_LABEL,
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("LinkedIn data accuracy check failed — BQ data does not match source API.")
        return result

    def dv360_standard_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table=f"dv360_transformed__{PLATFORM_LABEL}",
            table_name=f"dv360_standard__{PLATFORM_LABEL}",
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
            destination_table=f"dv360_transformed__{PLATFORM_LABEL}",
            table_name=f"dv360_youtube__{PLATFORM_LABEL}",
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

    def snapchat_comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table=f"snapchat_transformed__{PLATFORM_LABEL}",
            table_name=f"snapchat__{PLATFORM_LABEL}",
            source_name="snapchat",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name=COMPARISON_SECRET,
            project_id=env["PROJECT_ID"],
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError("Snapchat data accuracy check failed — BQ data does not match source API.")
        return result

    kube_cm360 = KubernetesPodOperator(
        name="cffc-cm360-transformation",
        task_id="cffc-cm360_transformation",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_cffc_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_cm360(),
        get_logs=True,
    )

    kube_facebook = KubernetesPodOperator(
        name="cffc-facebook-to-bigquery",
        task_id="cffc-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery", "dbt-bigquery:facebook_cffc_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_facebook(),
        get_logs=True,
    )

    kube_linkedin = KubernetesPodOperator(
        name="cffc-linkedin-to-bigquery",
        task_id="cffc-linkedin_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-linkedin-ads", "target-bigquery", "dbt-bigquery:linkedin_cffc_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_linkedin(),
        get_logs=True,
    )

    kube_snapchat = KubernetesPodOperator(
        name="cffc-snapchat-to-bigquery",
        task_id="cffc-snapchat_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-snapchat-ads", "target-bigquery", "dbt-bigquery:snapchat_cffc_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_snapchat(),
        get_logs=True,
    )

    kube_hivestack = KubernetesPodOperator(
        name="cffc-hivestack-to-bigquery",
        task_id="cffc-hivestack_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery", "dbt-bigquery:hivestack_cffc_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_hivestack(),
        get_logs=True,
    )

    kube_dv360 = KubernetesPodOperator(
        name="cffc-dv360-to-bigquery",
        task_id="cffc-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery", "dbt-bigquery:dv360_cffc_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dv360(),
        get_logs=True,
    )

    kube_ttd = KubernetesPodOperator(
        name="cffc-ttd-to-bigquery",
        task_id="cffc-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery", "dbt-bigquery:ttd_cffc_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_ttd(),
        get_logs=True,
        execution_timeout=datetime.timedelta(minutes=60),
    )

    kube_dash = KubernetesPodOperator(
        name="cffc-dash-to-bigquery-retirement_commission",
        task_id="cffc-dash_to_bigquery-retirement_commission",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule="all_done",
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_table__retirement_commission"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash(),
        get_logs=True,
    )

    kube_dash_union = KubernetesPodOperator(
        name="cffc-dash-union-to-bigquery-retirement_commission",
        task_id="cffc-dash_union_to_bigquery-retirement_commission",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_union__retirement_commission"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_dash(),
        get_logs=True,
    )

    task_facebook_comparison = PythonOperator(
        task_id="task_facebook_comparison",
        python_callable=facebook_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    task_linkedin_comparison = PythonOperator(
        task_id="task_linkedin_comparison",
        python_callable=linkedin_comparison_check,
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
    task_snapchat_comparison = PythonOperator(
        task_id="task_snapchat_comparison",
        python_callable=snapchat_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )

    kube_facebook >> task_facebook_comparison
    kube_linkedin >> task_linkedin_comparison
    kube_snapchat >> task_snapchat_comparison
    kube_cm360 >> [kube_dv360, kube_ttd]
    kube_dv360 >> [task_dv360_standard_comparison, task_dv360_youtube_comparison]
    [
        kube_facebook,
        kube_linkedin,
        kube_snapchat,
        kube_hivestack,
        kube_cm360,
        kube_dv360,
        kube_ttd,
    ] >> kube_dash >> kube_dash_union


# ---------------------------------------------------------------------------
# DAG 2: Google Ads (sorted brands) + GA4 (all dash brands)
# Flow: google_ads >> [dash, dash_search] >> dash_union >> [goal, session, keyword] >> ga4_final
# retirement_commission: GA4 only (dash_union from morning DAG)
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="cffc-meltano-google-ads",
    schedule_interval="0 14 * * *",
    default_args=default_args,
) as google_dag:

    def set_env_vars_google_ads_search(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"google_ads_search_transformed__{brand}"
        return env

    def set_env_vars_dash(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"dash_table__{brand}"
        return env

    def set_env_vars_dash_search(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"dash_table_search__{brand}"
        return env

    def set_env_vars_ga4_final(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"ga4_transformed__{brand}"
        return env

    def set_env_vars_ga4(brand, goal, property_id):
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
        env["DBT_BIGQUERY_AUTH_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
        env["DBT_BIGQUERY_DATASET"] = f"ga4_transformed__{brand}"
        env["TAP_GA4_PROPERTY_ID"] = property_id
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
        return env

    env_base = get_meltano_env()
    ga4_property_map = {
        "retirement_commission": env_base["TAP_GA4_PROPERTY_ID"],
        "sorted": env_base["TAP_GA4_PROPERTY_ID_SORTED"],
        "sorted_in_school": env_base["TAP_GA4_PROPERTY_ID_SORTED_IN_SCHOOL"],
    }

    for brand in GOOGLE_ADS_BRANDS:
        kube_google_ads = KubernetesPodOperator(
            name=f"cffc-google-ads-search-to-bigquery-{brand}",
            task_id=f"cffc-google-ads-search_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:google_ads_{brand}_models"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_google_ads_search(brand),
            get_logs=True,
        )

        kube_dash = KubernetesPodOperator(
            name=f"cffc-dash-to-bigquery-{brand}",
            task_id=f"cffc-dash_to_bigquery-{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule="all_done",
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        kube_dash_search = KubernetesPodOperator(
            name=f"cffc-dash-search-to-bigquery-{brand}",
            task_id=f"cffc-dash_search_to_bigquery-{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table_search__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash_search(brand),
            get_logs=True,
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"cffc-dash-union-to-bigquery-{brand}",
            task_id=f"cffc-dash_union_to_bigquery-{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{brand}"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        kube_ga4_final = KubernetesPodOperator(
            name=f"cffc-ga4-final-to-bigquery-{brand}",
            task_id=f"cffc-ga4_final_to_bigquery_{brand}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:ga4_{brand}_final_models"],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_ga4_final(brand),
            get_logs=True,
        )

        kube_google_ads >> [kube_dash, kube_dash_search]
        [kube_dash, kube_dash_search] >> kube_dash_union

        property_id = ga4_property_map[brand]
        for goal in ["goal", "session", "keyword"]:
            kube_ga4 = KubernetesPodOperator(
                name=f"cffc-ga4-{goal}-to-bigquery-{brand}",
                task_id=f"cffc-ga4_{goal}_to_bigquery_{brand}",
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
                env_vars=set_env_vars_ga4(brand, goal, property_id),
                get_logs=True,
            )
            kube_dash_union >> kube_ga4 >> kube_ga4_final

    # retirement_commission GA4 (dash_union produced by morning DAG)
    retirement = DASH_BRAND_RETIREMENT
    property_id = ga4_property_map[retirement]
    kube_ga4_final_rc = KubernetesPodOperator(
        name=f"cffc-ga4-final-to-bigquery-{retirement}",
        task_id=f"cffc-ga4_final_to_bigquery_{retirement}",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", f"dbt-bigquery:ga4_{retirement}_final_models"],
        container_resources=KUBE_RESOURCES,
        env_vars=set_env_vars_ga4_final(retirement),
        get_logs=True,
    )
    for goal in ["goal", "session", "keyword"]:
        kube_ga4 = KubernetesPodOperator(
            name=f"cffc-ga4-{goal}-to-bigquery-{retirement}",
            task_id=f"cffc-ga4_{goal}_to_bigquery_{retirement}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "run",
                "tap-ga4",
                "target-bigquery",
                f"dbt-bigquery:ga4_{retirement}_{goal}_models",
            ],
            container_resources=KUBE_RESOURCES,
            env_vars=set_env_vars_ga4(retirement, goal, property_id),
            get_logs=True,
        )
        kube_ga4 >> kube_ga4_final_rc
