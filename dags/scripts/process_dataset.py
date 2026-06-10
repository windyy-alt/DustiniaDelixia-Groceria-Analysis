import os
import pandas as pd
from clickhouse_driver import Client
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

DATA_DIR = "/opt/airflow/data"

TABLES = {
    "category_translation": {
        "file": "category_translation.csv",
        "ddl": """
            CREATE TABLE IF NOT EXISTS analytics.category_translation (
                product_category_name String,
                product_category_name_english String
            ) ENGINE = MergeTree()
            ORDER BY product_category_name
        """
    },


    "customers": {
        "file": "customers.csv",
        "ddl": """
            CREATE TABLE IF NOT EXISTS analytics.customers (
                customer_id String,
                customer_unique_id String,
                customer_zip_code_prefix Int64,
                customer_city String,
                customer_state String
            ) ENGINE = MergeTree()
            ORDER BY customer_id
        """
    },

    "geolocation": {
        "file": "geolocation.csv",
        "ddl": """
            CREATE TABLE IF NOT EXISTS analytics.geolocation (
                geolocation_zip_code_prefix Int64,
                geolocation_lat Float64,
                geolocation_lng Float64,
                geolocation_city String,
                geolocation_state String
            ) ENGINE = MergeTree()
            ORDER BY geolocation_zip_code_prefix
        """
    },

    "order_items": {
        "file": "order_items.csv",
        "ddl": """
            CREATE TABLE IF NOT EXISTS analytics.order_items (
                order_id String,
                order_item_id Int32,
                product_id String,
                seller_id String,
                shipping_limit_date Nullable(DateTime),
                price Float64,
                freight_value Float64
            ) ENGINE = MergeTree()
            ORDER BY (order_id, order_item_id)
        """
    },

    "order_payments": {
        "file": "order_payments.csv",
        "ddl": """
            CREATE TABLE IF NOT EXISTS analytics.order_payments (
                order_id String,
                payment_sequential Int32,
                payment_type String,
                payment_installments Int32,
                payment_value Float64
            ) ENGINE = MergeTree()
            ORDER BY (order_id, payment_sequential)
        """
    },


    "orders": {
        "file": "orders.csv",
        "ddl": """
            CREATE TABLE IF NOT EXISTS analytics.orders (
                order_id String,
                customer_id String,
                order_status String,
                order_purchase_timestamp Nullable(DateTime),
                order_approved_at Nullable(DateTime),
                order_delivered_carrier_date Nullable(DateTime),
                order_delivered_customer_date Nullable(DateTime),
                order_estimated_delivery_date Nullable(DateTime)
            ) ENGINE = MergeTree()
            ORDER BY order_id
        """
    },

    "products": {
        "file": "products.csv",
        "ddl": """
            CREATE TABLE IF NOT EXISTS analytics.products (
                product_id String,
                product_category_name Nullable(String),
                product_name_lenght Nullable(Float64),
                product_description_lenght Nullable(Float64),
                product_photos_qty Nullable(Float64),
                product_weight_g Nullable(Float64),
                product_length_cm Nullable(Float64),
                product_height_cm Nullable(Float64),
                product_width_cm Nullable(Float64)
            ) ENGINE = MergeTree()
            ORDER BY product_id
        """
    },

}


NUMERIC_COLUMNS = {

    "geolocation": [
        "geolocation_lat",
        "geolocation_lng"
    ],

    "order_items": [
        "order_item_id",
        "price",
        "freight_value"
    ],

    "order_payments": [
        "payment_sequential",
        "payment_installments",
        "payment_value"
    ],


    "products": [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]
}


DATE_COLUMNS = {

    "order_items": [
        "shipping_limit_date"
    ],


    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
}



def get_client():
    return Client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse-server'),
        user=os.getenv('CLICKHOUSE_USER', 'dilbi'),
        password=os.getenv('CLICKHOUSE_PASSWORD', 'rahasia')
    )

def load_all_tables():

    client = get_client()

    client.execute(
        'CREATE DATABASE IF NOT EXISTS analytics'
    )

    logger.info("Database analytics siap!")

    for table_name, config in TABLES.items():

        file_path = os.path.join(
            DATA_DIR,
            config["file"]
        )

        if not os.path.exists(file_path):
            logger.warning(
                f"File tidak ditemukan: {file_path}"
            )
            continue

        logger.info(
            f"Membaca {config['file']}..."
        )

        df = pd.read_csv(file_path)

        logger.info(f"----- {table_name} -----")

        for col in df.columns:
            logger.info(f"{col}: {df[col].dtype}")

        if table_name in NUMERIC_COLUMNS:

            for col in NUMERIC_COLUMNS[table_name]:

                if col in df.columns:

                    df[col] = pd.to_numeric(
                        df[col],
                        errors="coerce"
                    )

        if table_name in DATE_COLUMNS:

            for col in DATE_COLUMNS[table_name]:

                if col in df.columns:

                    df[col] = pd.to_datetime(
                        df[col],
                        errors="coerce"
                    )

        df = df.where(
            pd.notnull(df),
            None
        )

        for col in DATE_COLUMNS.get(table_name, []):

            if col in df.columns:

                df[col] = df[col].astype(object)

                df[col] = df[col].where(
                    pd.notnull(df[col]),
                    None
                )

        client.execute(config["ddl"])

        client.execute(
            f'TRUNCATE TABLE analytics.{table_name}'
        )

        for col in df.columns:
            sample_types = df[col].dropna().map(type).value_counts()

            logger.info(
                f"{table_name}.{col} -> {sample_types.to_dict()}"
            )

        data = [
            tuple(row)
            for row in df.itertuples(
                index=False,
                name=None
            )
        ]

        if data:
            client.execute(
                f'INSERT INTO analytics.{table_name} VALUES',
                data
            )

            logger.info(
                f"Berhasil load {len(data)} rows ke analytics.{table_name}"
            )

        else:
            logger.warning(
                f"Tidak ada data di {config['file']}"
            )

def create_rfm_table():
    client = get_client()

    # Buat tabel RFM
    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.customer_rfm (
            customer_unique_id String,
            customer_state String,
            customer_city String,
            last_order_date DateTime,
            recency_days Int32,
            frequency Int32,
            monetary Float64,
            r_score Int32,
            f_score Int32,
            m_score Int32,
            rfm_score Int32,
            rfm_segment String
        ) ENGINE = MergeTree()
        ORDER BY customer_unique_id
    """)

    client.execute('TRUNCATE TABLE analytics.customer_rfm')

    # Hitung RFM
    rfm_query = """
        WITH base AS (
            SELECT
                c.customer_unique_id,
                c.customer_state,
                c.customer_city,
                MAX(o.order_purchase_timestamp)         AS last_order_date,
                COUNT(DISTINCT o.order_id)              AS frequency,
                ROUND(SUM(op.payment_value), 2)         AS monetary
            FROM analytics.customers c
            JOIN analytics.orders o ON c.customer_id = o.customer_id
            JOIN analytics.order_payments op ON o.order_id = op.order_id
            WHERE o.order_status = 'delivered'
              AND o.order_purchase_timestamp IS NOT NULL
            GROUP BY c.customer_unique_id, c.customer_state, c.customer_city
        ),
        with_recency AS (
            SELECT *,
                dateDiff('day', last_order_date, now()) AS recency_days
            FROM base
        ),
        with_scores AS (
            SELECT *,
                -- R score: makin kecil recency makin bagus (skor 4)
                CASE
                    WHEN recency_days <= quantileExact(0.25)(recency_days) OVER () THEN 4
                    WHEN recency_days <= quantileExact(0.50)(recency_days) OVER () THEN 3
                    WHEN recency_days <= quantileExact(0.75)(recency_days) OVER () THEN 2
                    ELSE 1
                END AS r_score,
                -- F score: makin banyak order makin bagus (skor 4)
                CASE
                    WHEN frequency >= quantileExact(0.75)(frequency) OVER () THEN 4
                    WHEN frequency >= quantileExact(0.50)(frequency) OVER () THEN 3
                    WHEN frequency >= quantileExact(0.25)(frequency) OVER () THEN 2
                    ELSE 1
                END AS f_score,
                -- M score: makin besar monetary makin bagus (skor 4)
                CASE
                    WHEN monetary >= quantileExact(0.75)(monetary) OVER () THEN 4
                    WHEN monetary >= quantileExact(0.50)(monetary) OVER () THEN 3
                    WHEN monetary >= quantileExact(0.25)(monetary) OVER () THEN 2
                    ELSE 1
                END AS m_score
            FROM with_recency
        )
        SELECT
            customer_unique_id,
            customer_state,
            customer_city,
            last_order_date,
            recency_days,
            frequency,
            monetary,
            r_score,
            f_score,
            m_score,
            (r_score + f_score + m_score)   AS rfm_score,
            CASE
                WHEN (r_score + f_score + m_score) >= 11 THEN 'Champions'
                WHEN (r_score + f_score + m_score) >= 9  THEN 'Loyal'
                WHEN (r_score + f_score + m_score) >= 7  THEN 'Potential'
                WHEN (r_score + f_score + m_score) >= 5  THEN 'At Risk'
                ELSE 'Hibernating'
            END AS rfm_segment
        FROM with_scores
    """

    logger.info("Menghitung RFM...")
    rfm_data = client.execute(rfm_query)

    if rfm_data:
        client.execute(
            'INSERT INTO analytics.customer_rfm VALUES',
            rfm_data
        )
        logger.info(f"Berhasil load {len(rfm_data)} rows ke analytics.customer_rfm")
    else:
        logger.warning("Tidak ada data RFM!")

if __name__ == "__main__":
    load_all_tables()
    create_rfm_table()
