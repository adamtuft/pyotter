from __future__ import annotations

from typing import List, Tuple
import sqlite3

from otter.db.scripts import scripts

import otter.log


class DBChunkBuilder:
    """Builds a database representation of the chunks in a trace"""

    def __init__(self, con: sqlite3.Connection, bufsize: int = 100) -> None:
        self.debug = otter.log.log_with_prefix(
            f"[{self.__class__.__name__}]", otter.log.debug
        )
        self.con = con
        self.bufsize = bufsize
        self._buffer: List[Tuple[int, int, int]] = []

    def __len__(self) -> int:
        self._flush()
        row = self.con.execute(scripts["count_chunks"]).fetchone()
        return row["num_chunks"]

    def append_to_chunk(self, key: int, location_ref: int, location_count: int) -> None:
        self._buffer.append((key, location_ref, location_count))
        if len(self._buffer) > self.bufsize:
            self._flush()

    def close(self):
        self.debug("closing...")
        self._flush()

    def _flush(self):
        self.con.executemany(scripts["insert_chunk_events"], self._buffer)
        self.con.commit()
        self._buffer.clear()
