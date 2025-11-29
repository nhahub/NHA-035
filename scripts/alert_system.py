import smtplib
from email.mime.text import MIMEText
import mysql.connector as my
import yaml
import os
import logging

# --- Logger Setup ---
try:
    from logging_confg import setup_logger

    logger = setup_logger("alert_system")
except (ImportError, NameError):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("alert_system")


# --- Config Loader ---
def load_config(config_path):
    """Loads configuration from a YAML file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def check_and_send_alerts(config_path):
    """
    Finds unsent district notifications, FILTERS them against their stored threshold,
    sends emails, and updates their status.
    """
    # --- Load configuration ---
    try:
        config = load_config(config_path)
        db_conf = config["db"]
        email_conf = config.get("email_settings", {})

    except (FileNotFoundError, KeyError) as e:
        logger.critical(
            f"Critical error: Failed to load configuration. Error: {e}", exc_info=True
        )
        raise
    # --- Find districts requiring alerts ---
    conn = None
    all_pending_alerts = []
    try:
        conn = my.connect(**db_conf, charset="utf8mb4", collation="utf8mb4_unicode_ci")
        cursor = conn.cursor(dictionary=True)
        find_query = """
            SELECT 
                n.district_id, 
                n.alert_message, 
                n.num_neg_reviews, 
                n.threshold,
                ds.district,
                ds.governorate
            FROM notifications n
            JOIN district_stats ds ON n.district_id = ds.district_id
            WHERE n.send_status = 'no';
        """
        cursor.execute(find_query)
        all_pending_alerts = cursor.fetchall()

        if not all_pending_alerts:
            logger.info(
                "No pending district alerts found in the database. No alerts to send."
            )
            return

        logger.info(f"Found {len(all_pending_alerts )} districts requiring alerts.")

    except my.Error as e:
        logger.error(
            f"Failed to fetch districts from the database. Error: {e}", exc_info=True
        )
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()

    # --- Filter alerts where count >= stored threshold ---
    alerts_to_send = [
        alert
        for alert in all_pending_alerts
        if alert["num_neg_reviews"] >= alert["threshold"]
    ]

    if not alerts_to_send:
        logger.info(
            f"Found {len(all_pending_alerts)} pending alerts, but none meet their stored threshold."
        )
        return

    logger.info(
        f"Found {len(alerts_to_send)} alerts that meet their threshold. Attempting to send emails."
    )

    # ---  Try to send emails ---
    sent_district_ids = []
    try:
        smtp = smtplib.SMTP_SSL(
            email_conf.get("smtp_server", "smtp.gmail.com"),
            email_conf.get("smtp_port", 465),
        )
        smtp.login(email_conf["email"], email_conf["email_password"])

        for alert in alerts_to_send:
            district_id = alert["district_id"]

            final_message = (
                alert["alert_message"]
                + f" (This alert was triggered because the threshold is {alert['threshold']})."
            )

            msg = MIMEText(final_message, "plain", "utf-8")
            msg["Subject"] = (
                f"High Negative Reviews Alert: {alert['district']}, {alert['governorate']}"
            )
            msg["From"] = email_conf["email"]
            msg["To"] = ", ".join(email_conf["email_to"])

            smtp.send_message(msg)
            sent_district_ids.append(district_id)
            logger.info(f"Email sent for district_id: {district_id}")

        smtp.quit()
    except Exception as e:
        logger.error(f"Failed during email sending process. Error: {e}", exc_info=True)

    # ---  Update status for successfully sent emails ---
    if not sent_district_ids:
        logger.warning(
            "No emails were successfully sent, so no statuses will be updated."
        )
        return

    conn_update = None
    try:
        conn_update = my.connect(**db_conf, autocommit=True)
        cursor_update = conn_update.cursor()

        update_sql = (
            "UPDATE notifications SET send_status = 'yes' WHERE district_id = %s;"
        )
        update_data = [(nid,) for nid in sent_district_ids]

        cursor_update.executemany(update_sql, update_data)
        logger.info(
            f"Successfully updated status to 'yes' for {len(sent_district_ids)} notifications."
        )
    except my.Error as e:
        logger.error(
            f"Database error while updating send_status. Error: {e}", exc_info=True
        )
    finally:
        if conn_update and conn_update.is_connected():
            conn_update.close()
            logger.info("DB connection for update operation closed.")
            logger.info("--- Finished: Send Pending District Alerts ---")
