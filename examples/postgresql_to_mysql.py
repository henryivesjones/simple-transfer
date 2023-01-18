import logging

from easy_transfer import MySQLConnection, Pipeline, PostgreSQLConnection

logging.basicConfig(level=logging.INFO)


source_connection = PostgreSQLConnection(
    host="source-database.xxx.us-east-1.rds.amazonaws.com",
    port=5432,
    username="<username>",
    password="<password>",
    db="postgres",
)

destination_connection = MySQLConnection(
    host="destination-database.xxx.us-east-1.rds.amazonaws.com",
    port=3306,
    username="<username>",
    password="<password>",
    db="my_db",
)

pipeline = Pipeline(
    source_connection=source_connection,
    source_schema="public",
    source_table="source_table",
    destination_connection=destination_connection,
    destination_schema="my_db",
    destination_table="destination_table",
    inject_mode="swap",
)
pipeline.execute()
