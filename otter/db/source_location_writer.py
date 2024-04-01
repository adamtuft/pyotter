from typing import Dict

from .types import SourceLocation
from .connect import Connection

import otter.log


class DBSourceLocationWriter:

    def __init__(
        self,
        con: Connection,
        string_id_lookup: Dict[str, int],
        source_location_id: Dict[SourceLocation, int],
    ) -> None:
        self.debug = otter.log.log_with_prefix(
            f"[{self.__class__.__name__}]", otter.log.debug
        )
        self._con = con
        self._string_id_lookup = string_id_lookup
        self._source_location_id = source_location_id

    def __iter__(self):
        self.debug("%d items:", len(self._source_location_id))
        for location, loc_id in self._source_location_id.items():
            self.debug(
                f"{loc_id=}, {location=}, file_id={self._string_id_lookup[location.file]}, func_id={self._string_id_lookup[location.func]}"
            )
            yield (
                loc_id,
                self._string_id_lookup[location.file],
                self._string_id_lookup[location.func],
                location.line,
            )

    def close(self):
        self._con.executemany("insert into source values(?,?,?,?);", self)
        self._con.commit()
