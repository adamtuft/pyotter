from typing import List, Tuple, Any

import otter.log

from ..connect import Connection


class BufferedDBWriter:

    def __init__(
        self,
        con: Connection,
        table: str,
        nargs: int,
        bufsize: int,
        overwrite: bool,
    ) -> None:
        placeholder = ",".join("?" * nargs)
        self.sql_insert_row = f"insert into {table} values({placeholder});"
        self.sql_count_rows = f"select count(*) from {table};"
        prefix = f"[{self.__class__.__name__}({table=})]"
        self.debug = otter.log.log_with_prefix(prefix, otter.log.debug)
        self.info = otter.log.log_with_prefix(prefix, otter.log.info)
        self.con = con
        self._bufsize = bufsize
        self._buffer: List[Tuple[Any]] = []
        if overwrite:
            self.debug(f"delete from {table}")
            self.con.execute(f"delete from {table};")

    def insert(self, *args: Any):
        self._buffer.append(args)
        if len(self._buffer) >= self._bufsize:
            self._flush()

    def close(self):
        self._flush()
        if otter.log.is_debug_enabled():
            (rows,) = self.con.execute(self.sql_count_rows).fetchone()
            self.debug("contains %d rows", rows)

    def _flush(self):
        self.debug(f"write {len(self._buffer)} records")
        self.con.executemany(self.sql_insert_row, self._buffer)
        self.con.commit()
        self._buffer.clear()
