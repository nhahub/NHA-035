import os
import pandas as pd
import regex as re
import numpy as np


try:
    from logging_confg import setup_logger

    logger = setup_logger("preprocess_text")
except (ImportError, NameError):
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("preprocess_text")


def clean_arabic_text(text):
    """Cleans Arabic text by removing non-Arabic characters and extra whitespace."""
    if pd.isna(text):
        return np.nan
    text = str(text)
    # Remove non-Arabic characters, keeping only letters and spaces
    text = re.sub(r"[^ء-ي\s]", "", text)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else np.nan


def preprocess_text_batch(
    df,
    col_name,
    batch_number,
    output_dir=None,
):
    """
    Cleans a batch of text data, drops empty rows, and saves the result.
    """
    logger.info(f"Starting preprocessing for batch {batch_number}")

    # ---- Input validation ----
    if df.empty:
        logger.warning("Input DataFrame is empty. Skipping preprocessing.")
        return None

    if col_name not in df.columns:
        logger.error(f"Column '{col_name}' not found in DataFrame.", exc_info=True)
        raise KeyError(f"The specified column '{col_name}' does not exist.")

    if output_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(script_dir, "..", "outputs", "clean_batches")

    logger.info(f"Initial rows in batch {batch_number}: {len(df)}")

    # ---- Text Cleaning ----
    df_clean = df.copy()
    df_clean["clean_review"] = df_clean[col_name].apply(clean_arabic_text)

    #  Drop Empty Rows
    df_clean.dropna(subset=["clean_review"], inplace=True)

    if df_clean.empty:
        logger.warning(f"Batch {batch_number} is empty after cleaning.")
        return None

    logger.info(f"Batch {batch_number} cleaned. Rows remaining: {len(df_clean)}")

    # Save the cleaned batch
    os.makedirs(output_dir, exist_ok=True)
    cleaned_batch_file = os.path.join(output_dir, f"cleaned_batch_{batch_number}.csv")

    try:
        df_clean.to_csv(cleaned_batch_file, index=False)
        logger.info(f"Cleaned batch {batch_number} saved to: {cleaned_batch_file}")
    except Exception as e:
        logger.error(f"Failed to save cleaned batch {batch_number}: {e}", exc_info=True)
        raise

    logger.info(f"Preprocessing completed for batch {batch_number}")
    return cleaned_batch_file
