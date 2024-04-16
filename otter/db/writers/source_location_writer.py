from typing import Dict

from ..connect import Connection
from ..types import SourceLocation

import otter.log


class SourceLocationWriter:
    """NOTE: Doesn't use BufferedDBWriter as there should be few enough source
    locations that we can just buffer them in memory."""

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
            yield (
                loc_id,
                self._string_id_lookup[location.file],
                self._string_id_lookup[location.func],
                location.line,
            )

    def close(self):
        self.debug("closing...")
        self._con.executemany("insert into source values(?,?,?,?);", self)
        self._con.commit()
