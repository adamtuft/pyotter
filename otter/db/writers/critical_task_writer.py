from sqlite3 import Connection

from .buffered_writers import BufferedDBWriter


class CritTaskWriter(BufferedDBWriter):

    def __init__(self, con: Connection, bufsize: int = 1000) -> None:
        super().__init__(con, "critical_task", 3, bufsize)

    def insert(self, task: int, sequence: int, critical_child: int, /, *args):
        return super().insert(task, sequence, critical_child)
