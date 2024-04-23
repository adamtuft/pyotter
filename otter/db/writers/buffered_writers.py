from typing import List, Tuple, Any
from sqlite3 import Connection

from .writer_base import WriterBase


class BufferedDBWriter(WriterBase):

    def __init__(
        self,
        con: Connection,
        table: str,
        nargs: int,
        bufsize: int,
    ) -> None:
        placeholder = ",".join("?" * nargs)
        self.sql_insert_row = f"insert into {table} values({placeholder});"
        self._con = con
        self._bufsize = bufsize
        self._buffer: List[Tuple[Any]] = []

    def insert(self, *args: Any):
        self._buffer.append(args)
        if len(self._buffer) >= self._bufsize:
            self._flush()

    def close(self):
        self._flush()

    def _flush(self):
        self.log_debug(f"write {len(self._buffer)} records")
        self._con.executemany(self.sql_insert_row, self._buffer)
        self._con.commit()
        self._buffer.clear()
