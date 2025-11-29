import os
import sys
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.append("/opt/airflow/project/scripts")
sys.path.append("/opt/airflow/project/config")

from logging_confg import setup_logger

logger = setup_logger("sentiment_pipeline_dag")

CONFIG_PATH = "/opt/airflow/project/config/setting.yaml"


def load_config():
    """Loads and returns the project's YAML configuration."""
    import yaml

    if not os.path.exists(CONFIG_PATH):
        logger.error(f"FATAL: Config file not found: {CONFIG_PATH}")
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def send_email(subject, body):
    """
    Sends a notification email using settings from the 'setting.yaml' config file.
    This function is now self-contained and reads directly from the config file.
    """
    import smtplib
    from email.mime.text import MIMEText

    # --- THE FIX IS HERE ---

    try:

        config = load_config()

        email_conf = config.get("email_settings", {})

        if not all(k in email_conf for k in ["email", "email_password", "email_to"]):
            logger.warning(
                "Email settings are incomplete or missing in setting.yaml. Cannot send email."
            )
            return

        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = email_conf["email"]
        msg["To"] = ", ".join(email_conf["email_to"])

        smtp_server = email_conf.get("smtp_server", "smtp.gmail.com")
        smtp_port = email_conf.get("smtp_port", 465)

        with smtplib.SMTP_SSL(smtp_server, smtp_port) as smtp:
            smtp.login(email_conf["email"], email_conf["email_password"])
            smtp.send_message(msg)

        logger.info(f"Email with subject '{subject}' sent successfully.")

    except FileNotFoundError:
        logger.error("Could not find setting.yaml file. Cannot send email.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while sending email. Error: {e}")


def notify_task_failure(context):
    """Callback function for INDIVIDUAL task failures."""

    task_instance = context["task_instance"]
    subject = f"Airflow Task Failed: {task_instance.dag_id}.{task_instance.task_id}"
    body = f"""
    Hi Team,
    A task has failed in the Airflow pipeline.

    DAG: {task_instance.dag_id}
    Task: {task_instance.task_id}
    Execution Date: {context['execution_date']}
    Reason: {context.get('exception')}

    Please check the logs for more details: {task_instance.log_url}
    """
    send_email(subject, body)


def send_dag_success_email(**kwargs):
    """Sends a final email for a successful DAG run."""

    dag_run = kwargs["dag_run"]
    subject = f"Airflow DAG Success: {dag_run.dag_id}"
    body = f"The DAG '{dag_run.dag_id}' completed successfully for execution date: {dag_run.execution_date}"
    send_email(subject, body)


def send_dag_failure_email(**kwargs):
    """Sends a final summary email for a failed DAG run."""

    dag_run = kwargs["dag_run"]
    subject = f"Airflow DAG Failed: {dag_run.dag_id}"
    body = f"The DAG '{dag_run.dag_id}' failed for execution date: {dag_run.execution_date}. Please check individual task failure emails for details."
    send_email(subject, body)


#  DAG Definition
default_args = {
    "owner": "Sahar",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="supplier_sentiment_pipeline",
    description="An ETL pipeline for sentiment analysis with detailed alerting.",
    start_date=datetime(2024, 6, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["sentiment", "etl", "branching-alert"],
) as dag:

    #  Task Definitions
    # Heavy imports are moved inside each function to prevent DAG timeouts.

    def extract_task_py(**kwargs):
        from extract_data import extract_batch

        ti = kwargs["ti"]
        config = load_config()
        batch_number = int(Variable.get("sentiment_batch_number", default_var=1))
        df_batch_path = extract_batch(
            file_name="reviews_data.csv",
            batch_number=batch_number,
            batch_size=config["batch"]["batch_size"],
            raw_data_dir=config["paths"]["raw_data_dir"],
            output_dir=config["paths"]["temp_batches_dir"],
        )
        if df_batch_path is None:
            logger.info(f"End of file reached. Resetting batch counter.")
            Variable.set("sentiment_batch_number", 1)
            ti.xcom_push(key="status", value="EOF")
        else:
            Variable.set("sentiment_batch_number", batch_number + 1)
            ti.xcom_push(key="dataframe_path", value=df_batch_path)
            ti.xcom_push(key="batch_number", value=batch_number)

    def transform_task_py(**kwargs):
        from transform_data import preprocess_text_batch
        import pandas as pd

        ti = kwargs["ti"]
        if ti.xcom_pull(task_ids="extract_task", key="status") == "EOF":
            return
        dataframe_path = ti.xcom_pull(task_ids="extract_task", key="dataframe_path")
        batch_number = ti.xcom_pull(task_ids="extract_task", key="batch_number")
        config = load_config()
        df = pd.read_csv(dataframe_path)
        df_cleaned_path = preprocess_text_batch(
            df,
            col_name="review",
            batch_number=batch_number,
            output_dir=config["paths"]["clean_batches_dir"],
        )
        ti.xcom_push(key="cleaned_dataframe_path", value=df_cleaned_path)

    def predict_task_py(**kwargs):
        from model_utilies import predict_sentiment_batch
        import pandas as pd

        ti = kwargs["ti"]
        if ti.xcom_pull(task_ids="extract_task", key="status") == "EOF":
            return
        cleaned_path = ti.xcom_pull(
            task_ids="transform_task", key="cleaned_dataframe_path"
        )
        if not cleaned_path:
            return
        batch_number = ti.xcom_pull(task_ids="extract_task", key="batch_number")
        config = load_config()
        df_cleaned = pd.read_csv(cleaned_path)
        predict_path = predict_sentiment_batch(
            df=df_cleaned,
            model_path="/opt/airflow/project/model/marbert_model",
            tokenizer_path="/opt/airflow/project/model/tokenizer",
            batch_number=batch_number,
            output_dir=config["paths"]["predict_batches_dir"],
            text_column="clean_review",
        )
        ti.xcom_push(key="predicted_batch_path", value=predict_path)

    def load_task_py(**kwargs):
        from load_data import load_batch_to_db
        import pandas as pd

        ti = kwargs["ti"]
        if ti.xcom_pull(task_ids="extract_task", key="status") == "EOF":
            return
        predict_path = ti.xcom_pull(task_ids="predict_task", key="predicted_batch_path")
        if not predict_path:
            return
        df = pd.read_csv(predict_path)
        config = load_config()
        batch_number = ti.xcom_pull(task_ids="extract_task", key="batch_number")

        alert_threshold = config.get("alert_threshold", 10)
        load_batch_to_db(
            df,
            db_conf=config["db"],
            batch_number=batch_number,
            threshold=alert_threshold,
        )

    def alert_task_py(**kwargs):
        from alert_system import check_and_send_alerts

        if kwargs["ti"].xcom_pull(task_ids="extract_task", key="status") == "EOF":
            return

        check_and_send_alerts(CONFIG_PATH)

    #  Task Instantiation (Operators)
    extract_op = PythonOperator(
        task_id="extract_task",
        python_callable=extract_task_py,
        on_failure_callback=notify_task_failure,
    )
    transform_op = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task_py,
        on_failure_callback=notify_task_failure,
    )
    predict_op = PythonOperator(
        task_id="predict_task",
        python_callable=predict_task_py,
        on_failure_callback=notify_task_failure,
    )
    load_op = PythonOperator(
        task_id="load_task",
        python_callable=load_task_py,
        on_failure_callback=notify_task_failure,
    )

    alert_op = PythonOperator(
        task_id="alert_task",
        python_callable=alert_task_py,
        on_failure_callback=notify_task_failure,
    )

    dag_success_op = PythonOperator(
        task_id="dag_success_email",
        python_callable=send_dag_success_email,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    dag_failure_op = PythonOperator(
        task_id="dag_failure_email",
        python_callable=send_dag_failure_email,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # DAG dependencies
    main_chain = [extract_op, transform_op, predict_op, load_op, alert_op]
    for i in range(len(main_chain) - 1):
        main_chain[i] >> main_chain[i + 1]

    # Branching dependency for final notifications, triggered after the main chain ends.
    alert_op >> [dag_success_op, dag_failure_op]
