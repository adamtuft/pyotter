from typing import Dict

from .connect import Connection

import otter.log


class DBStringDefinitionWriter:

    def __init__(
        self,
        con: Connection,
        string_id_lookup: Dict[str, int],
    ) -> None:
        self.debug = otter.log.log_with_prefix(
            f"[{self.__class__.__name__}]", otter.log.debug
        )
        self._con = con
        self._string_id_map = string_id_lookup

    def __iter__(self):
        for string, string_key in self._string_id_map.items():
            yield string_key, string

    def close(self):
        self.debug("closing...")
        self._con.executemany("insert into string values(?,?);", self)
        self._con.commit()
