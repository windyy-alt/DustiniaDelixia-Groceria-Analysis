import os
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

DATA_DIR = "/opt/airflow/data"

REQUIRED_FILES = [
    "category_translation.csv",
    "closed_deals.csv",
    "customers.csv",
    "geolocation.csv",
    "mql.csv",
    "order_items.csv",
    "order_payments.csv",
    "order_reviews.csv",
    "orders.csv",
    "products.csv",
    "sellers.csv"
]


def extract_dataset():
    logger.info("Extracting dataset files...")

    missing_files = []

    for file_name in REQUIRED_FILES:
        file_path = os.path.join(DATA_DIR, file_name)

        if os.path.exists(file_path):

            try:
                df = pd.read_csv(file_path)

                file_size_mb = round( 
                    os.path.getsize(file_path) / (1024 * 1024),
                    2
                )

                logger.info(
                    f"FOUND : {file_name} | "
                    f"Rows={len(df)} | "
                    f"Cols={len(df.columns)} | "
                    f"Size={file_size_mb} MB"
                )

            except Exception as e:
                logger.error(f"ERROR    : {file_name} - {str(e)}")
                missing_files.append(file_name)

        else:
            logger.error(f"MISSING  : {file_name}")
            missing_files.append(file_name)

    if missing_files:
        raise FileNotFoundError(
            f"Missing dataset files: {missing_files}"
        )

    logger.info("All dataset files validated successfully!")

if __name__ == "__main__":
    extract_dataset()