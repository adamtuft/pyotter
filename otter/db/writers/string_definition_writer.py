from typing import Dict

from ..connect import Connection

import otter.log


class StringDefinitionWriter:
    """NOTE: Doesn't use BufferedDBWriter as there should be few enough string
    definitions that we can just buffer them in memory."""

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

    def close(self):
        self.debug("closing...")
        if otter.log.is_debug_enabled():
            for key, value in self._string_id_map.items():
                self.debug(f"{key=}, {value=}")
        items = ((k, s) for s, k in self._string_id_map.items())
        self._con.executemany("insert into string values(?,?);", items)
        self._con.commit()
