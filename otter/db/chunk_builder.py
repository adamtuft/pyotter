from __future__ import annotations

from typing import Protocol, List, Tuple
import sqlite3

from otter.db.scripts import scripts

import otter.log

from ..core.events import Event


class ChunkKeyNotFoundError(Exception):

    def __init__(self, key: int) -> None:
        super().__init__(f"chunk key not found: {key}")


class ChunkKeyDuplicateError(Exception):

    def __init__(self, key: int) -> None:
        super().__init__(f"chunk key duplicated: {key}")


class ChunkBuilderProtocol(Protocol):
    """Capable of building the set of chunks from a trace"""

    def __len__(self) -> int: ...

    def new_chunk(
        self, key: int, event: Event, location_ref: int, location_count: int
    ): ...

    def append_to_chunk(
        self, key: int, event: Event, location_ref: int, location_count: int
    ) -> None: ...

    def contains(self, key: int) -> bool: ...

    def close(self): ...


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

    def new_chunk(self, key: int, event: Event, location_ref: int, location_count: int):
        if self.contains(key):
            raise ChunkKeyDuplicateError(key)
        self._append_to_chunk(key, event, location_ref, location_count)

    def append_to_chunk(
        self, key: int, event: Event, location_ref: int, location_count: int
    ) -> None:
        if not self.contains(key):
            raise ChunkKeyNotFoundError(key)
        self._append_to_chunk(key, event, location_ref, location_count)

    def _append_to_chunk(
        self, key: int, event: Event, location_ref: int, location_count: int
    ) -> None:
        self._buffer.append((key, location_ref, location_count))
        if len(self._buffer) > self.bufsize:
            self._flush()

    def contains(self, key: int) -> bool:
        for k, *_ in self._buffer:
            if k == key:
                return True
        rows = self.con.execute(scripts["get_chunk_events"], (key,)).fetchall()
        return len(rows) > 0

    def close(self):
        self.debug("closing...")
        self._flush()

    def _flush(self):
        self.con.executemany(scripts["insert_chunk_events"], self._buffer)
        self.con.commit()
        self._buffer.clear()