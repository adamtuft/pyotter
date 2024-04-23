from sqlite3 import Connection

from .buffered_writers import BufferedDBWriter


class CritTaskWriter(BufferedDBWriter):

    def __init__(self, con: Connection, /, *, sim_id: int, bufsize: int = 1000) -> None:
        super().__init__(con, "critical_task", 4, bufsize)
        self._sim_id = sim_id

    def insert(self, task: int, sequence: int, critical_child: int, /, *args):
        return super().insert(self._sim_id, task, sequence, critical_child)
