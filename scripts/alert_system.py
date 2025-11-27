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


# --- Main Function ---
def check_and_send_alerts(config_path):
    """
    Checks for products requiring alerts, sends emails, and updates their status.
    """
    # --- Load configuration ---
    try:
        config = load_config(config_path)
        db_conf = config["db"]
        email_conf = config.get("email_settings", {})
        threshold = config.get("alert_threshold", 10)
    except (FileNotFoundError, KeyError) as e:
        logger.critical(
            f"Critical error: Failed to load configuration. Error: {e}", exc_info=True
        )
        raise

    # --- Find products requiring alerts ---
    conn = None
    all_pending_alerts = []
    try:
        conn = my.connect(**db_conf, charset="utf8mb4", collation="utf8mb4_unicode_ci")
        cursor = conn.cursor(dictionary=True)
        find_query = "SELECT notification_id, product_id, message, num_negative, threshold FROM notification WHERE send_status = 'no';"
        cursor.execute(find_query)
        all_pending_alerts = cursor.fetchall()
        cursor.close()
        conn.close()

        if not all_pending_alerts:
            logger.info("No pending alerts found in the database. No alerts to send.")
            return

        logger.info(f"Found {len(all_pending_alerts )} products requiring alerts.")

    except my.Error as e:
        logger.error(
            f"Failed to fetch pending alerts from the database. Error: {e}",
            exc_info=True,
        )
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()

    # --- Filter alerts where count >= its stored threshold ---
    alerts_to_send = [
        alert
        for alert in all_pending_alerts
        if alert["num_negative"] >= alert["threshold"]
    ]

    if not alerts_to_send:
        logger.info(
            f"Found {len(all_pending_alerts)} pending alerts, but none currently meet their stored threshold."
        )
        return
    logger.info(
        f"Found {len(alerts_to_send)} alerts that meet their individual thresholds. Attempting to send emails."
    )

    # --- Send emails ---
    sent_notification_ids = []
    try:
        smtp = smtplib.SMTP_SSL(
            email_conf.get("smtp_server", "smtp.gmail.com"),
            email_conf.get("smtp_port", 465),
        )
        smtp.login(email_conf["email"], email_conf["email_password"])

        for alert in alerts_to_send:
            notification_id = alert["notification_id"]
            final_message = (
                alert["message"]
                + f" (This alert was triggered because the count reached the threshold of {alert['threshold']})."
            )

            msg = MIMEText(final_message, "plain", "utf-8")
            msg["Subject"] = f"High Negative Reviews Alert: ID {alert['product_id']}"
            msg["From"] = email_conf["email"]
            msg["To"] = ", ".join(email_conf["email_to"])

            smtp.send_message(msg)
            sent_notification_ids.append(notification_id)
            logger.info(f"Email sent for notification_id: {notification_id}")

        smtp.quit()
    except Exception as e:
        logger.error(
            f"Failed during the email sending process. Error: {e}", exc_info=True
        )

    # ---Update notification status in DB ---
    if not sent_notification_ids:
        logger.warning("No emails successfully sent, no database updates performed.")
        return

    try:
        conn = my.connect(**db_conf, charset="utf8mb4", collation="utf8mb4_unicode_ci")
        cursor = conn.cursor()

        update_sql = (
            "UPDATE notification SET send_status = 'yes' WHERE notification_id = %s;"
        )
        update_data = [(nid,) for nid in sent_notification_ids]

        cursor.executemany(update_sql, update_data)
        conn.commit()
        logger.info(
            f"Successfully updated status to 'yes' for {len(sent_notification_ids)} notifications."
        )
    except my.Error as e:
        logger.error(
            f"Database error while updating send_status. Error: {e}", exc_info=True
        )
    finally:
        if conn and conn.is_connected():
            conn.close()
            logger.info("DB connection for update operation closed.")
            logger.info("Finished: Send Pending Alerts.")
