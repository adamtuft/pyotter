from ..connect import Connection

from .buffered_writers import BufferedDBWriter


class ChunkWriter(BufferedDBWriter):
    """Builds a database representation of the chunks in a trace"""

    def __init__(
        self, con: Connection, bufsize: int = 1000, overwrite: bool = True
    ) -> None:
        super().__init__(con, "chunk_contents", 3, bufsize, overwrite)

    def insert(
        self, key: int, location_ref: int, location_count: int, /, *args
    ) -> None:
        return super().insert(key, location_ref, location_count)
