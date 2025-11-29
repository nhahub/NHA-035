import pandas as pd
import mysql.connector as my

# --- Setup logger ---
try:
    from logging_confg import setup_logger

    logger = setup_logger("load_data")
except (ImportError, NameError):
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("load_data")


def load_batch_to_db(df_batch, db_conf, batch_number, threshold):
    """
    Loads a predicted batch into the database using a single transaction
    and separate cursors for each operation to prevent 'Commands out of sync' errors.
    """
    if not isinstance(df_batch, pd.DataFrame) or df_batch.empty:
        logger.warning(f"Batch {batch_number} has no data. Skipping database load.")
        return

    conn = None
    try:
        # ----  Connect to DB and start transaction ----
        conn = my.connect(**db_conf, autocommit=False)
        logger.info(f"Batch {batch_number}: Connected to DB. Transaction started.")

        # ---  Aggregate stats and UPSERT into district_stats ---
        stats_df = (
            df_batch.groupby(["governorate", "district"])
            .agg(
                avg_rate=("stars", "mean"),
                total_reviews=("review", "count"),
                num_positive=("predicted_sentiment", lambda x: (x == "Positive").sum()),
                num_negative=("predicted_sentiment", lambda x: (x == "Negative").sum()),
                num_neutral=("predicted_sentiment", lambda x: (x == "Neutral").sum()),
            )
            .reset_index()
        )

        stats_data = [
            tuple(x)
            for x in stats_df[
                [
                    "governorate",
                    "district",
                    "avg_rate",
                    "total_reviews",
                    "num_positive",
                    "num_negative",
                    "num_neutral",
                ]
            ].values
        ]

        upsert_stats_sql = """
            INSERT INTO district_stats (governorate, district, avg_rates, total_reviews, num_pos_reviews, num_neg_reviews, num_nat_reviews)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_reviews = total_reviews + VALUES(total_reviews),
                num_pos_reviews = num_pos_reviews + VALUES(num_pos_reviews),
                num_nat_reviews = num_nat_reviews + VALUES(num_nat_reviews),
                num_neg_reviews = num_neg_reviews + VALUES(num_neg_reviews),
                avg_rates = ((district_stats.avg_rates * district_stats.total_reviews) + (VALUES(avg_rates) * VALUES(total_reviews))) / (district_stats.total_reviews + VALUES(total_reviews));
        """

        if stats_data:
            cursor1 = conn.cursor()
            cursor1.executemany(upsert_stats_sql, stats_data)
            logger.info(
                f"Batch {batch_number}: Upserted {cursor1.rowcount} records into 'district_stats'."
            )
            cursor1.close()

        # ---Fetch newly created/updated district_ids ---
        cursor2 = conn.cursor()
        districts_to_fetch = [
            tuple(d) for d in stats_df[["governorate", "district"]].values
        ]
        district_map = {}
        if districts_to_fetch:
            placeholders = ", ".join(["(%s, %s)"] * len(districts_to_fetch))
            flat_values = [item for d in districts_to_fetch for item in d]
            select_sql = f"SELECT district_id, governorate, district FROM district_stats WHERE (governorate, district) IN ({placeholders})"
            cursor2.execute(select_sql, flat_values)

            results = cursor2.fetchall()
            district_map = {(gov, dist): id for id, gov, dist in results}
        cursor2.close()

        # --- Prepare and INSERT individual reviews ---
        df_batch["district_id"] = df_batch.apply(
            lambda row: district_map.get((row["governorate"], row["district"])), axis=1
        )
        df_batch.dropna(subset=["district_id"], inplace=True)
        df_batch["district_id"] = df_batch["district_id"].astype(int)

        review_data = [
            tuple(x)
            for x in df_batch[["district_id", "review", "predicted_sentiment"]].values
        ]

        if review_data:
            cursor3 = conn.cursor()
            insert_reviews_sql = """
                INSERT INTO reviews (district_id, review_text, predicted_sentiment)
                VALUES (%s, %s, %s);
            """
            cursor3.executemany(insert_reviews_sql, review_data)
            logger.info(f"Inserted {cursor3.rowcount} records into 'reviews'.")
            cursor3.close()

        # ---Insert districts with negative reviews to the notification table ---
        cursor4 = conn.cursor(dictionary=True)
        districts_in_batch = list(district_map.values())
        if districts_in_batch:
            placeholders = ", ".join(["%s"] * len(districts_in_batch))
            get_neg_districts_sql = f"SELECT district_id, district, governorate, num_neg_reviews FROM district_stats WHERE district_id IN ({placeholders}) AND num_neg_reviews > 0;"
            cursor4.execute(get_neg_districts_sql, districts_in_batch)
            districts_to_notify = cursor4.fetchall()

            if districts_to_notify:
                logger.info(
                    f"Found {len(districts_to_notify)} districts with negative reviews. Logging to notifications table with threshold ({threshold})."
                )
                notification_data = []
                for d in districts_to_notify:
                    message = f"District '{d['district']}' in {d['governorate']} has {d['num_neg_reviews']} negative review(s)."
                    notification_data.append(
                        (d["district_id"], message, d["num_neg_reviews"], threshold)
                    )

                noification_sql = """
                    INSERT INTO notifications (district_id, alert_message, num_neg_reviews, threshold)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        alert_message = VALUES(alert_message),
                        num_neg_reviews = VALUES(num_neg_reviews),
                        threshold = VALUES(threshold),
                        send_status = 'no';
                """
                cursor4.executemany(noification_sql, notification_data)
                logger.info(
                    f"Logged/updated {cursor4.rowcount} alerts in 'notifications' table."
                )
        cursor4.close()

        # --- Commit Transaction ---
        conn.commit()
        logger.info(f"Batch {batch_number}: Transaction committed successfully.")

    except my.Error as e:
        logger.error(
            f"Database error in batch {batch_number}. Rolling back. Error: {e}"
        )
        if conn:
            conn.rollback()
        raise
    finally:

        if conn and conn.is_connected():
            conn.close()
            logger.info(f"Batch {batch_number}: DB connection closed.")
