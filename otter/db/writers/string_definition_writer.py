from typing import Dict
from sqlite3 import Connection

from otter.log import is_debug_enabled

from .writer_base import WriterBase


class StringDefinitionWriter(WriterBase):
    """NOTE: Doesn't use BufferedDBWriter as there should be few enough string
    definitions that we can just buffer them in memory."""

    def __init__(
        self,
        con: Connection,
        string_id_lookup: Dict[str, int],
    ) -> None:
        self._con = con
        self._string_id_map = string_id_lookup

    def close(self):
        self.log_debug("closing...")
        if is_debug_enabled():
            for key, value in self._string_id_map.items():
                self.log_debug(f"{key=}, {value=}")
        items = ((k, s) for s, k in self._string_id_map.items())
        self._con.executemany("insert into string values(?,?);", items)
        self._con.commit()
