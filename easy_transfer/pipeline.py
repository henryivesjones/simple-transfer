import logging
from typing import Literal, Optional

from easy_transfer import EASY_TRANSFER_CONFIG, Connection, Extractor, Injector


class Pipeline:
    def __init__(
        self,
        source_connection: Connection,
        source_schema: str,
        source_table: str,
        destination_connection: Connection,
        destination_schema: str,
        destination_table: str,
        intermediate_location: str = "",
        transport_params: Optional[dict] = None,
        inject_mode: Literal["overwrite", "append", "swap"] = "overwrite",
    ):
        self.source_connection = source_connection
        self.source_schema = source_schema
        self.source_table = source_table

        self.destination_connection = destination_connection
        self.destination_schema = destination_schema
        self.destination_table = destination_table

        self.intermediate_location = intermediate_location
        self.transport_params = transport_params

        self.inject_mode: Literal["overwrite", "append", "swap"] = inject_mode

    def execute(self):
        if EASY_TRANSFER_CONFIG.VERBOSE:
            logging.info(
                "Executing pipeline from "
                f"source ({self.source_connection} `{self.source_schema}`.`{self.source_table}`)"
                f" to destination ({self.destination_connection} `{self.destination_schema}`.`{self.destination_table}`)"
                f" using intermediate location `{self.intermediate_location}`"
                f" and injection mode `{self.inject_mode}`"
            )
        with Extractor(
            self.source_connection,
            self.source_schema,
            self.source_table,
            self.intermediate_location,
        ) as e:
            e.extract()
            ddl_file = e.ddl_destination
            csv_file = e.csv_destination

        with Injector(
            self.destination_connection,
            self.destination_schema,
            self.destination_table,
            csv_file,
            ddl_file,
        ) as i:
            i.inject(self.inject_mode)
