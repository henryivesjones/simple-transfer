import logging

from easy_transfer import EASY_TRANSFER_CONFIG, Pipeline, PostgreSQLConnection

logging.basicConfig(level=logging.INFO)
EASY_TRANSFER_CONFIG.VERBOSE = True
EASY_TRANSFER_CONFIG.BATCH_SIZE = 50_000


source_connection = PostgreSQLConnection(
    host="",
    port=5432,
    username="",
    password="",
    db="postgres",
)

destination_connection = PostgreSQLConnection(
    host="",
    port=5432,
    username="",
    password="",
    db="postgres",
)

pipeline = Pipeline(
    source_connection=source_connection,
    source_schema="public",
    source_table="",
    destination_connection=destination_connection,
    destination_schema="public",
    destination_table="",
    intermediate_location="output",
    inject_mode="swap",
)
pipeline.execute()
