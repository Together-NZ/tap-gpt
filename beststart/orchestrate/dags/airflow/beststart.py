import datetime
import logging
from copy import deepcopy
from datetime import timedelta

import pendulum
from airflow import models
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from comparison_package import ComparisonTrigger
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from kubernetes.client import models as k8s_models


IMAGE = "australia-southeast1-docker.pkg.dev/best-start-main/meltano/meltano-best-start-main:prod"
PROJECT_NAME = "best-start-main"
COMPARISON_SECRET = "airflow-variables-meltano_beststart_main"
BRANDS = ["beststart", "hr_career"]

REPAIR_DAG_ID = "beststart-comparison-repair"
# Repair extracts must cover the whole comparison window, otherwise drift older
# than the normal 13-day extract window can never be corrected by a re-run.
REPAIR_WINDOW_DAYS = 30

log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
comparison_start_date = (
    datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
).strftime("%Y-%m-%d")

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz),
}


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_beststart_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=13)
    ).strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def get_ga4_start_date():
    return (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
    ).strftime("%Y-%m-%d")


def set_env_vars_facebook():
    env = get_meltano_env()
    env["BQ_DATASET"] = "facebook_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "facebook_transformed"
    return env


def set_env_vars_tiktok():
    env = get_meltano_env()
    env["BQ_DATASET"] = "tiktok_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "tiktok_transformed"
    return env


def set_env_vars_cm360():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "cm360_transformed"
    return env


def set_env_vars_dv360():
    env = get_meltano_env()
    env["BQ_DATASET"] = "dv360_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "dv360_transformed"
    return env


def set_env_vars_google_ads_search(label):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"google_ads_search_transformed__{label}"
    return env


def set_env_vars_dash():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "dash_table"
    env["PLAN_CODE"] = "bs"
    return env


def set_env_vars_dash_search(label):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = f"dash_table_search__{label}"
    return env


def set_env_vars_dash_search_union():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "dash_table_search"
    return env


def set_env_vars_ga4(goal):
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
    env["BQ_DATASET"] = "ga4_raw"
    env["BQ_METHOD"] = "gcs_stage"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "ga4_transformed"
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
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = PROJECT_NAME
    env["DBT_BIGQUERY_DATASET"] = "ga4_transformed"
    return env


def widen_to_repair_window(env):
    env["START_DATE"] = (
        datetime.datetime.now(local_tz) - datetime.timedelta(days=REPAIR_WINDOW_DAYS)
    ).strftime("%Y-%m-%d")
    return env


def make_comparison_check(destination_table, table_name, source_name, label):
    def comparison_check(**context):
        env = get_meltano_env()
        trigger = ComparisonTrigger(
            project_name=PROJECT_NAME,
            destination_table=destination_table,
            table_name=table_name,
            source_name=source_name,
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name=COMPARISON_SECRET,
            project_id=env["PROJECT_ID"],
        )
        result = trigger.compare_data()
        if not result:
            raise ValueError(
                f"{label} data accuracy check failed — BigQuery does not match the source API."
            )
        return result

    return comparison_check


def make_repair_trigger(task_id, platform):
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=REPAIR_DAG_ID,
        conf={"platform": platform},
        trigger_run_id=f"repair_{platform}__{{{{ run_id }}}}",
        reset_dag_run=True,
        wait_for_completion=False,
        trigger_rule="one_failed",
        retries=0,
    )


# ---------------------------------------------------------------------------
# DAG 1: Social / display
# Flow: CM360 -> DV360; platforms -> dash_table -> dash_union.
# Every client DAG ends with dash_table + dash_union; dash_table_search only
# when the DAG also runs Google Ads (see DAG 2).
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="beststart-meltano-extraction-transformation-dbt",
    schedule_interval="0 4 * * *",
    default_args=default_args,
) as dag:
    kube_facebook = KubernetesPodOperator(
        name="beststart-facebook-to-bigquery",
        task_id="beststart-facebook_to_bigquery",
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
    kube_cm360 = KubernetesPodOperator(
        name="beststart-cm360-to-bigquery",
        task_id="beststart-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360(),
        get_logs=True,
    )
    kube_dv360 = KubernetesPodOperator(
        name="beststart-dv360-to-bigquery",
        task_id="beststart-dv360_to_bigquery",
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
    kube_dash = KubernetesPodOperator(
        name="beststart-dash-to-bigquery",
        task_id="beststart-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule="all_done",
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_table",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        get_logs=True,
    )
    kube_dash_union = KubernetesPodOperator(
        name="beststart-dash-union-to-bigquery",
        task_id="beststart-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_union",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        get_logs=True,
    )

    task_facebook_comparison = PythonOperator(
        task_id="task_facebook_comparison",
        python_callable=make_comparison_check(
            "facebook_transformed", "facebook", "meta", "Facebook"
        ),
        retries=0,
        trigger_rule="all_done",
    )
    task_dv360_standard_comparison = PythonOperator(
        task_id="task_dv360_standard_comparison",
        python_callable=make_comparison_check(
            "dv360_transformed", "dv360_standard", "dv360_standard", "DV360 standard"
        ),
        retries=0,
        trigger_rule="all_done",
    )
    task_dv360_youtube_comparison = PythonOperator(
        task_id="task_dv360_youtube_comparison",
        python_callable=make_comparison_check(
            "dv360_transformed", "dv360_youtube", "dv360_youtube", "DV360 YouTube"
        ),
        retries=0,
        trigger_rule="all_done",
    )

    trigger_repair_facebook = make_repair_trigger(
        "trigger_repair_facebook", "facebook"
    )
    trigger_repair_dv360 = make_repair_trigger("trigger_repair_dv360", "dv360")

    kube_facebook >> task_facebook_comparison >> trigger_repair_facebook
    kube_cm360 >> kube_dv360
    kube_dv360 >> [task_dv360_standard_comparison, task_dv360_youtube_comparison]
    [
        task_dv360_standard_comparison,
        task_dv360_youtube_comparison,
    ] >> trigger_repair_dv360
    [kube_facebook, kube_cm360, kube_dv360] >> kube_dash >> kube_dash_union


# ---------------------------------------------------------------------------
# DAG 2: TikTok / Google Ads / dash search / GA4
# Has Google Ads → includes dash_table_search (+ search union) as well as
# dash_table + dash_union (required on every DAG).
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="beststart-meltano-google-ads",
    schedule_interval="00 14 * * *",
    default_args=default_args,
) as google_dag:
    kube_tiktok = KubernetesPodOperator(
        name="beststart-tiktok-to-bigquery",
        task_id="beststart-tiktok_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-tiktok",
            "target-bigquery",
            "--full-refresh",
            "dbt-bigquery:tiktok_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_tiktok(),
        get_logs=True,
    )
    kube_dash = KubernetesPodOperator(
        name="beststart-dash-to-bigquery",
        task_id="beststart-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule="all_done",
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_table",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        get_logs=True,
    )
    kube_dash_search_union = KubernetesPodOperator(
        name="beststart-dash-search-union-to-bigquery",
        task_id="beststart-dash_search_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_table_search",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search_union(),
        get_logs=True,
    )
    kube_dash_union = KubernetesPodOperator(
        name="beststart-dash-union-to-bigquery",
        task_id="beststart-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "invoke",
            "dbt-bigquery",
            "run",
            "--select",
            "dash_union",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        get_logs=True,
    )
    kube_ga4_final = KubernetesPodOperator(
        name="beststart-ga4-final-to-bigquery",
        task_id="beststart-ga4_final_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:ga4_final_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4_final(),
        get_logs=True,
    )

    task_tiktok_comparison = PythonOperator(
        task_id="task_tiktok_comparison",
        python_callable=make_comparison_check(
            "tiktok_transformed", "tiktok", "tiktok", "TikTok"
        ),
        retries=0,
        trigger_rule="all_done",
    )
    trigger_repair_tiktok = make_repair_trigger("trigger_repair_tiktok", "tiktok")

    google_ads_tasks = []
    dash_search_tasks = []
    for label in BRANDS:
        kube_google_ads = KubernetesPodOperator(
            name=f"beststart-google-ads-search-to-bigquery-{label}",
            task_id=f"beststart-google_ads_search_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                f"dbt-bigquery:google_ads_{label}_models",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(label),
            get_logs=True,
        )
        kube_dash_search = KubernetesPodOperator(
            name=f"beststart-dash-search-to-bigquery-{label}",
            task_id=f"beststart-dash_search_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=[
                "--environment=prod",
                "invoke",
                "dbt-bigquery",
                "run",
                "--select",
                f"dash_table_search__{label}",
            ],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(label),
            get_logs=True,
        )
        kube_google_ads >> kube_dash_search
        google_ads_tasks.append(kube_google_ads)
        dash_search_tasks.append(kube_dash_search)

    ga4_tasks = []
    for goal in ["goal", "session", "keyword"]:
        kube_ga4 = KubernetesPodOperator(
            name=f"beststart-ga4-{goal}-to-bigquery",
            task_id=f"beststart-ga4_{goal}_to_bigquery",
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
        ga4_tasks.append(kube_ga4)

    kube_tiktok >> task_tiktok_comparison >> trigger_repair_tiktok
    [*google_ads_tasks, kube_tiktok] >> kube_dash
    dash_search_tasks >> kube_dash_search_union
    [kube_dash, kube_dash_search_union] >> kube_dash_union
    for task in ga4_tasks:
        kube_dash_union >> task >> kube_ga4_final


# ---------------------------------------------------------------------------
# DAG 3: comparison repair (triggered only)
# Re-extracts one platform over the full comparison window, then re-checks once.
# A failure here is not transient — it needs a human, so nothing re-triggers.
# ---------------------------------------------------------------------------
REPAIR_ENTRYPOINTS = {
    "facebook": "repair_facebook_extract",
    "tiktok": "repair_tiktok_extract",
    "dv360": "repair_cm360_transform",
}


def choose_repair_platform(**context):
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run and dag_run.conf else {}) or {}
    platform = conf.get("platform")
    if platform not in REPAIR_ENTRYPOINTS:
        raise ValueError(
            f"Repair DAG needs conf {{'platform': one of "
            f"{sorted(REPAIR_ENTRYPOINTS)}}}, got {platform!r}"
        )
    log.info("Repairing platform %s", platform)
    return REPAIR_ENTRYPOINTS[platform]


with models.DAG(
    dag_id=REPAIR_DAG_ID,
    schedule_interval=None,
    default_args={**default_args, "retries": 1},
    max_active_runs=1,
) as repair_dag:
    choose_platform = BranchPythonOperator(
        task_id="choose_repair_platform",
        python_callable=choose_repair_platform,
        retries=0,
    )

    repair_facebook = KubernetesPodOperator(
        name="beststart-repair-facebook-to-bigquery",
        task_id="repair_facebook_extract",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-facebook",
            "target-bigquery",
            "--full-refresh",
            "dbt-bigquery:facebook_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=widen_to_repair_window(set_env_vars_facebook()),
        get_logs=True,
    )
    recheck_facebook = PythonOperator(
        task_id="recheck_facebook",
        python_callable=make_comparison_check(
            "facebook_transformed", "facebook", "meta", "Facebook (after repair)"
        ),
        retries=0,
    )

    repair_tiktok = KubernetesPodOperator(
        name="beststart-repair-tiktok-to-bigquery",
        task_id="repair_tiktok_extract",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-tiktok",
            "target-bigquery",
            "--full-refresh",
            "dbt-bigquery:tiktok_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=widen_to_repair_window(set_env_vars_tiktok()),
        get_logs=True,
    )
    recheck_tiktok = PythonOperator(
        task_id="recheck_tiktok",
        python_callable=make_comparison_check(
            "tiktok_transformed", "tiktok", "tiktok", "TikTok (after repair)"
        ),
        retries=0,
    )

    # DV360 classification reads cm360_direct_buy, so rebuild it first — same
    # ordering as DAG 1.
    repair_cm360 = KubernetesPodOperator(
        name="beststart-repair-cm360-to-bigquery",
        task_id="repair_cm360_transform",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360(),
        get_logs=True,
    )
    repair_dv360 = KubernetesPodOperator(
        name="beststart-repair-dv360-to-bigquery",
        task_id="repair_dv360_extract",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=[
            "--environment=prod",
            "run",
            "tap-dv360",
            "target-bigquery",
            "--full-refresh",
            "dbt-bigquery:dv360_models",
        ],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=widen_to_repair_window(set_env_vars_dv360()),
        get_logs=True,
    )
    recheck_dv360_standard = PythonOperator(
        task_id="recheck_dv360_standard",
        python_callable=make_comparison_check(
            "dv360_transformed",
            "dv360_standard",
            "dv360_standard",
            "DV360 standard (after repair)",
        ),
        retries=0,
    )
    recheck_dv360_youtube = PythonOperator(
        task_id="recheck_dv360_youtube",
        python_callable=make_comparison_check(
            "dv360_transformed",
            "dv360_youtube",
            "dv360_youtube",
            "DV360 YouTube (after repair)",
        ),
        retries=0,
    )

    choose_platform >> repair_facebook >> recheck_facebook
    choose_platform >> repair_tiktok >> recheck_tiktok
    choose_platform >> repair_cm360 >> repair_dv360
    repair_dv360 >> [recheck_dv360_standard, recheck_dv360_youtube]
