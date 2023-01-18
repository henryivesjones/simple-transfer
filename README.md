# easy-transfer
Easy data transfers between relational databases. Specify the source/destination connection, schema, and table and easy-transfer does the rest.

# Quickstart
```python
import logging
from easy_transfer import Pipeline, PostgreSQLConnection

logging.basicConfig(level=logging.INFO)

source_connection = PostgreSQLConnection(
    host="source-database.xxx.us-east-1.rds.amazonaws.com",
    port=5432,
    username="<username>",
    password="<password>",
    db="postgres",
)

destination_connection = PostgreSQLConnection(
    host="destination-database.xxx.us-east-1.rds.amazonaws.com",
    port=5432,
    username="<username>",
    password="<password>",
    db="postgres",
)

pipeline = Pipeline(
    source_connection=source_connection,
    source_schema="public",
    source_table="source_table",
    destination_connection=destination_connection,
    destination_schema="public",
    destination_table="destination_table",
    inject_mode="swap",
)
pipeline.execute()

```

# Supported Databases
 - PostgreSQL
 - MySQL

# Supported temporary storage locations
This package utilizes the [`smart_open`](https://github.com/RaRe-Technologies/smart_open) package when opening a file object, this enables the intermediate storage location to be anything supported by this library:
 - Local File System
 - AWS S3
 - Azure Blob Storage
 - GCP Cloud Storage

# Injection Methods
## overwrite
This mode will drop and recreate the table if it exists before inserting the data into it.
## append
This mode will create the table if it doesn't exist, and then insert the data into it.
## swap
This mode will create a swap table and insert the data into it. Then in a transaction it will swap the names of the swap table and existing table and then drop the now renamed existing table.
