import os
import sys
import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

sys.path.append("/opt/airflow/model")

# --- Setup logger ---
try:
    from logging_confg import setup_logger

    logger = setup_logger("predict_sentiment")
except (ImportError, NameError):
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("predict_sentiment")


def predict_sentiment_batch(
    df,
    model_path,
    tokenizer_path,
    batch_number,
    output_dir=None,
    text_column="clean_review",
    micro_batch_size=32,
):
    """
    Loads a model, predicts sentiment for a batch, and saves the result.
    """

    logger.info(f"Starting prediction for batch {batch_number}")

    # ---- Input validation ----
    if df.empty:
        logger.warning("Input DataFrame is empty. Skipping prediction.")
        return None

    if output_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(script_dir, "..", "outputs", "predict_batches")
    os.makedirs(output_dir, exist_ok=True)

    # ---- Load Model & Tokenizer ----
    logger.info(f"Loading model and tokenizer from: {model_path}")
    try:
        tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, local_files_only=True)
        model = AutoModelForSequenceClassification.from_pretrained(
            model_path, local_files_only=True
        )
        logger.info("Model and tokenizer loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load model from {model_path}: {e}", exc_info=True)
        raise

    # ---- Prepare data ----
    if text_column not in df.columns:
        logger.error(f"Column '{text_column}' not found in DataFrame.", exc_info=True)
        raise KeyError(f"Column '{text_column}' not found in DataFrame.")

    df_clean = df.dropna(subset=[text_column]).copy()
    texts = df_clean[text_column].astype(str).tolist()

    if not texts:
        logger.warning("No valid texts to predict after dropping NaNs.")
        return None

    # ---- Perform prediction ----
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    logger.info(f"Predicting {len(texts)} texts on device: {device}")

    arabic_id2label = {2: "Positive", 1: "Neutral", 0: "Negative"}
    all_preds = []

    model.eval()
    with torch.no_grad():
        for i in range(0, len(texts), micro_batch_size):
            micro_batch_texts = texts[i : i + micro_batch_size]
            inputs = tokenizer(
                micro_batch_texts,
                padding=True,
                truncation=True,
                max_length=256,
                return_tensors="pt",
            ).to(device)
            outputs = model(**inputs)
            preds = torch.argmax(outputs.logits, dim=1).cpu().tolist()
            all_preds.extend(preds)

    # ---- Save results ----
    df_clean["predicted_sentiment"] = [
        arabic_id2label.get(p, "unknown") for p in all_preds
    ]

    output_path = os.path.join(output_dir, f"predicted_batch_{batch_number}.csv")
    try:
        df_clean.to_csv(output_path, index=False, encoding="utf-8-sig")
        logger.info(f"Predicted batch {batch_number} saved to: {output_path}")
    except Exception as e:
        logger.error(
            f"Failed to save predicted batch {batch_number}: {e}", exc_info=True
        )
        raise

    logger.info(f"Prediction completed for batch {batch_number}")
    return output_path
