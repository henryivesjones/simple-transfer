import json
import logging
import os
from typing import Literal, Optional, TextIO, Union
from uuid import uuid4

import smart_open

from easy_transfer.config import EASY_TRANSFER_CONFIG
from easy_transfer.connection import Column, Connection


class Injector:
    ddl_file: TextIO
    csv_file: TextIO

    def __init__(
        self,
        connection: Connection,
        schema: str,
        table: str,
        source_csv_location: str,
        source_ddl_location: str,
        transport_params: Optional[dict] = None,
    ):
        self.connection = connection
        self.schema = schema
        self.table = table
        self.source_csv_location = source_csv_location
        self.source_ddl_location = source_ddl_location
        self.transport_params = transport_params

    def __enter__(self):
        self.connection.connect()
        self.csv_file = smart_open.open(
            self.source_csv_location, "r", transport_params=self.transport_params
        )
        if EASY_TRANSFER_CONFIG.VERBOSE:
            logging.info(f"Opened file `{self.source_csv_location}`")
        self.ddl_file = smart_open.open(
            self.source_ddl_location, "r", transport_params=self.transport_params
        )
        if EASY_TRANSFER_CONFIG.VERBOSE:
            logging.info(f"Opened file `{self.source_ddl_location}`")
        return self

    def __exit__(self, type, value, traceback):
        self.connection.close()
        self.csv_file.close()
        self.ddl_file.close()
        if EASY_TRANSFER_CONFIG.VERBOSE:
            logging.info(f"Closed file `{self.source_csv_location}`")
            logging.info(f"Closed file `{self.source_ddl_location}`")

    def inject(self, mode: Literal["overwrite", "append", "swap"] = "overwrite"):
        columns = [Column(**c) for c in json.loads(self.ddl_file.read())]
        if mode == "swap":
            swap_id = uuid4().hex[:12]
            swap_table = f"{self.table}__{swap_id}"
            old_table = f"{self.table}__old"
            if EASY_TRANSFER_CONFIG.VERBOSE:
                logging.info(
                    f"Creating and inserting data into swap table `{self.schema}`.`{swap_table}`"
                )

            self.connection.execute(
                self.connection.generate_create_table_statement(
                    self.schema, swap_table, columns
                )
            )
            self.connection.import_csv(self.schema, swap_table, self.csv_file)
            if EASY_TRANSFER_CONFIG.VERBOSE:
                logging.info(
                    f"Performing swap from `{self.schema}`.`{swap_table}` to `{self.schema}`.`{self.table}`"
                )
            self.connection.execute_many(
                [
                    self.connection.generate_rename_table_statement(
                        self.schema, self.table, old_table
                    ),
                    self.connection.generate_rename_table_statement(
                        self.schema, swap_table, self.table
                    ),
                    self.connection.generate_drop_table_statement(
                        self.schema, old_table
                    ),
                ],
                [[], [], []],
            )
            return
        if mode == "overwrite":
            if EASY_TRANSFER_CONFIG.VERBOSE:
                logging.info(f"Dropping table `{self.schema}`.`{self.table}`")
            self.connection.execute(
                self.connection.generate_drop_table_statement(self.schema, self.table)
            )
        if EASY_TRANSFER_CONFIG.VERBOSE:
            logging.info(
                f"Creating and inserting data into table `{self.schema}`.`{self.table}`"
            )
        self.connection.execute(
            self.connection.generate_create_table_statement(
                self.schema, self.table, columns
            )
        )
        self.connection.import_csv(self.schema, self.table, self.csv_file)
