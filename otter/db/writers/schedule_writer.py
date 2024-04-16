from otter.definitions import TaskAction

from ..connect import Connection

from .buffered_writers import BufferedDBWriter


class ScheduleWriter(BufferedDBWriter):

    def __init__(
        self, con: Connection, bufsize: int = 1000, overwrite: bool = True
    ) -> None:
        super().__init__(con, "sim_task_history", 4, bufsize, overwrite)

    def insert(
        self,
        task: int,
        action: TaskAction,
        event_ts: int,
        /,
        *args,
    ):
        super().insert(task, action.value, event_ts, -1)
