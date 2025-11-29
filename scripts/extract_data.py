import os
import pandas as pd


# Assuming logging_confg.py and setup_logger are available
try:
    from logging_confg import setup_logger

    logger = setup_logger("extract_data")
except (ImportError, NameError):
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("extract_data")


def extract_batch(
    file_name,
    batch_number=1,
    batch_size=500,
    raw_data_dir=None,
    output_dir=None,
):
    """
    Extracts a single batch from a CSV file and saves it.
    This version is simplified for clarity and robustness.
    """
    #  Validate inputs and set up paths
    if batch_number < 1:
        logger.error(f"Invalid batch_number: {batch_number}. Must be 1 or greater.")
        return None

    # ---- Define paths ----
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if raw_data_dir is None:
        raw_data_dir = os.path.join(script_dir, "..", "Data", "raw")
    if output_dir is None:
        output_dir = os.path.join(script_dir, "..", "outputs", "temp_batches")

    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(raw_data_dir, file_name)
    if not os.path.exists(file_path):
        logger.error(f"Source file not found: {file_path}")
        raise FileNotFoundError(f"Source file does not exist: {file_path}")

    # Read the specific batch efficiently
    try:
        start_row = 1 + (batch_number - 1) * batch_size

        # Read header first
        header = pd.read_csv(file_path, nrows=0).columns.tolist()
        logger.info(f"File header columns: {header}")

        # Read the actual data chunk.
        batch_df = pd.read_csv(
            file_path,
            header=None,  # Tell pandas there's no header in the chunk we're reading.
            names=header,  # Apply the correct header we just read.
            skiprows=start_row,
            nrows=batch_size,
        )
        logger.info(f"Batch {batch_number} read successfully with {len(batch_df)} rows")

    except pd.errors.EmptyDataError:
        logger.warning(f"File '{file_name}' is empty.")
        return None
    except Exception as e:
        logger.error(f"Error reading CSV file '{file_name}': {e}", exc_info=True)
        raise

    # ---- Save the batch ----
    if batch_df.empty:
        logger.info(f"Batch {batch_number} is empty (end of file reached).")
        return None

    batch_file_path = os.path.join(output_dir, f"batch_{batch_number}.csv")
    try:
        batch_df.to_csv(batch_file_path, index=False)
        logger.info(
            f"Batch {batch_number} with {len(batch_df)} rows saved to {batch_file_path}"
        )
    except Exception as e:
        logger.error(
            f"Failed to save batch {batch_number} to {batch_file_path}: {e}",
            exc_info=True,
        )
        raise

    logger.info(f"Extraction completed for batch {batch_number}")
    return batch_file_path
