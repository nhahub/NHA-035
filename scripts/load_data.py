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


def clean_and_convert_to_numeric(series):
    """
    Cleans and converts a pandas Series to numeric:
    - Removes commas
    - Converts non-convertible values to NaN
    """
    series_str = series.astype(str).str.strip()
    series_no_commas = series_str.str.replace(",", "", regex=False)
    return pd.to_numeric(series_no_commas, errors="coerce")


def load_batch_to_db(df_batch, db_conf, batch_number, threshold):
    """
    Loads a predicted batch into the database:
    - Cleans numeric columns
    - Aggregates product statistics
    - UPSERTs product table and inserts reviews
    """
    if not isinstance(df_batch, pd.DataFrame) or df_batch.empty:
        logger.warning(f"Batch {batch_number} has no data. Skipping database load.")
        return

    conn = None
    cursor = None
    try:
        # ---- Clean numeric columns ----
        logger.info(f"Batch {batch_number}: Cleaning 'Price' and 'Stars' columns.")
        df_batch["Price"] = clean_and_convert_to_numeric(df_batch["Price"])
        df_batch["Stars"] = clean_and_convert_to_numeric(df_batch["Stars"])
        df_batch.dropna(subset=["Price", "Stars"], inplace=True)

        if df_batch.empty:
            logger.warning(
                f"Batch {batch_number} is empty after cleaning numeric columns. Skipping DB load."
            )
            return

        # ---- Connect to DB and start transaction ----
        conn = my.connect(**db_conf, autocommit=False)
        cursor = conn.cursor(dictionary=True)
        logger.info(f"Batch {batch_number}: Connected to DB. Transaction started.")

        # ---- Aggregate product statistics ----
        product_agg = (
            df_batch.groupby(["ASIN", "Title"])
            .agg(
                avg_price=("Price", "mean"),
                avg_rates=("Stars", "mean"),
                total_reviews=("Review", "count"),
                num_positive=("predicted_sentiment", lambda x: (x == "إيجابي").sum()),
                num_negative=("predicted_sentiment", lambda x: (x == "سلبي").sum()),
                num_neutral=("predicted_sentiment", lambda x: (x == "محايد").sum()),
            )
            .reset_index()
        )

        # ---- Prepare and UPSERT product data ----
        prod_data = [
            tuple(x)
            for x in product_agg[
                [
                    "ASIN",
                    "Title",
                    "avg_price",
                    "avg_rates",
                    "num_positive",
                    "num_neutral",
                    "num_negative",
                    "total_reviews",
                ]
            ].values
        ]

        upsert_prod_sql = """
            INSERT INTO product (
                product_id, product_name, AVG_price, AVG_rates,
                num_pos_reviews, num_nat_reviews, num_neg_reviews, total_reviews
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                product_name = VALUES(product_name),
                num_pos_reviews = num_pos_reviews + VALUES(num_pos_reviews),
                num_nat_reviews = num_nat_reviews + VALUES(num_nat_reviews),
                num_neg_reviews = num_neg_reviews + VALUES(num_neg_reviews),
                AVG_price = ((product.AVG_price * product.total_reviews) + (VALUES(AVG_price) * VALUES(total_reviews))) / (product.total_reviews + VALUES(total_reviews)),
                AVG_rates = ((product.AVG_rates * product.total_reviews) + (VALUES(AVG_rates) * VALUES(total_reviews))) / (product.total_reviews + VALUES(total_reviews)),
                total_reviews = total_reviews + VALUES(total_reviews);
        """
        if prod_data:
            cursor.executemany(upsert_prod_sql, prod_data)
            logger.info(
                f"Batch {batch_number}: Upserted {cursor.rowcount} records into 'product' table."
            )

        # ---- Insert individual reviews ----
        df_batch["predicted_sentiment"] = df_batch["predicted_sentiment"].str.strip()
        logger.info(
            f"Batch {batch_number}: Preview 'predicted_sentiment' values: {df_batch['predicted_sentiment'].head(5).to_list()}"
        )
        review_data = [
            (row["Review"], row["ASIN"], row["predicted_sentiment"])
            for _, row in df_batch.dropna(subset=["Review"]).iterrows()
        ]
        insert_reviews_sql = """
            INSERT INTO reviews (review_text, prod_id, predicted_sentiment)
            VALUES (%s, %s, %s)
        """
        if review_data:
            cursor.executemany(insert_reviews_sql, review_data)
            logger.info(
                f"Batch {batch_number}: Inserted {cursor.rowcount} records into 'reviews' table."
            )

        # --- Insert into notification table ---
        product_ids_in_batch = product_agg["ASIN"].unique().tolist()
        if product_ids_in_batch:
            placeholders = ", ".join(["%s"] * len(product_ids_in_batch))
            get_neg_products_sql = f"""
                SELECT product_id, product_name, num_neg_reviews 
                FROM product 
                WHERE product_id IN ({placeholders}) AND num_neg_reviews > 0;
            """

            cursor.execute(get_neg_products_sql, product_ids_in_batch)
            products_to_notify = cursor.fetchall()

            if products_to_notify:
                logger.info(
                    f"Found {len(products_to_notify)} products with negative reviews. Logging to notification table with current threshold ({threshold})."
                )

                notification_data = []
                for p in products_to_notify:
                    message = f"Product '{p['product_name']}' (ID: {p['product_id']}) has received {p['num_neg_reviews']} negative review(s)."
                    notification_data.append(
                        (p["product_id"], message, p["num_neg_reviews"], threshold)
                    )

                insert_notification_sql = """
                            INSERT INTO notification (product_id, message, num_negative,threshold)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                message = VALUES(message),
                                num_negative = VALUES(num_negative),
                                threshold = VALUES(threshold),
                                send_status = 'no'; 

                        """

                cursor.executemany(insert_notification_sql, notification_data)
                logger.info(
                    f"Logged/updated {cursor.rowcount} alerts in 'notification' table."
                )

        cursor.close()

        # ---- Commit transaction ----
        conn.commit()
        logger.info(f"Batch {batch_number}: Transaction committed successfully.")

    except my.Error as e:
        logger.error(
            f"Database error in batch {batch_number}. Rolling back.", exc_info=True
        )
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.critical(
            f"Unexpected error in batch {batch_number}. Rolling back.", exc_info=True
        )
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info(f"Batch {batch_number}: DB connection closed.")
