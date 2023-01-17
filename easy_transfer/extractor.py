import csv
import json
import logging
import os
from typing import Optional, TextIO

import smart_open

from easy_transfer.config import EASY_TRANSFER_CONFIG
from easy_transfer.connection import Connection


class Extractor:
    csv_file: TextIO
    ddl_file: TextIO

    def __init__(
        self,
        connection: Connection,
        schema: str,
        table: str,
        destination: str,
        transport_params: Optional[dict] = None,
    ):
        self.connection = connection
        self.schema = schema
        self.table = table
        self.csv_destination = os.path.join(
            destination, f"{schema.lower()}__{table.lower()}.csv"
        )
        self.ddl_destination = os.path.join(
            destination, f"DDL_{schema.lower()}__{table.lower()}.json"
        )
        self.transport_params = transport_params

    def __enter__(self):
        self.connection.connect()
        self.csv_file = smart_open.open(
            self.csv_destination, "w", transport_params=self.transport_params
        )
        if EASY_TRANSFER_CONFIG.VERBOSE:
            logging.info(f"Opened file `{self.csv_destination}`")
        self.ddl_file = smart_open.open(
            self.ddl_destination, "w", transport_params=self.transport_params
        )
        if EASY_TRANSFER_CONFIG.VERBOSE:
            logging.info(f"Opened file `{self.ddl_destination}`")
        return self

    def __exit__(self, type, value, traceback):
        self.connection.close()
        self.csv_file.close()
        self.ddl_file.close()
        if EASY_TRANSFER_CONFIG.VERBOSE:
            logging.info(f"Closed file `{self.csv_destination}`")
            logging.info(f"Closed file `{self.ddl_destination}`")

    def extract(self):
        if EASY_TRANSFER_CONFIG.VERBOSE:
            logging.info(
                f"Extracting columns from information schema for `{self.schema}`.`{self.table}`"
            )
        columns = self.connection.extract_table_ddl(self.schema, self.table)
        self.ddl_file.write(json.dumps([c.to_dict() for c in columns]))
        if EASY_TRANSFER_CONFIG.VERBOSE:
            logging.info(
                f"Extracting data from `{self.schema}`.`{self.table}` to `{self.csv_destination}`"
            )
        writer = csv.writer(self.csv_file, delimiter=",")
        rows_generator = self.connection.extract_table(self.schema, self.table)
        writer.writerow([c.name for c in columns])
        for row in rows_generator:
            writer.writerow(row)
