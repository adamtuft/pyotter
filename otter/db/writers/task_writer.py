from typing import Dict, Optional

from otter.definitions import TaskAction

import otter.log

from ..types import SourceLocation
from ..connect import Connection
from .buffered_writers import BufferedDBWriter


class TaskMetaWriter:

    def __init__(
        self,
        con: Connection,
        string_id_lookup: Dict[str, int],
        bufsize: int = 1000,
        overwrite: bool = True,
    ) -> None:
        self.debug = otter.log.log_with_prefix(
            f"[{self.__class__.__name__}]", otter.log.debug
        )
        self._string_id_lookup = string_id_lookup
        self._task_meta = BufferedDBWriter(
            con, "task", 10, bufsize=bufsize, overwrite=overwrite
        )
        self._task_links = BufferedDBWriter(
            con, "task_relation", 2, bufsize=bufsize, overwrite=overwrite
        )

    def add_task_metadata(self, task: int, parent: Optional[int], label: str) -> None:
        self._task_meta.insert(
            task,
            parent,
            None,
            self._string_id_lookup[label],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        if parent is not None:
            self._task_links.insert(parent, task)

    def close(self):
        self.debug("closing...")
        self._task_meta.close()
        self._task_links.close()


class TaskActionWriter:

    def __init__(
        self,
        con: Connection,
        source_location_id: Dict[SourceLocation, int],
        bufsize: int = 1000,
        overwrite: bool = True,
    ) -> None:
        self.debug = otter.log.log_with_prefix(
            f"[{self.__class__.__name__}]", otter.log.debug
        )
        self._source_location_id = source_location_id
        self._task_actions = BufferedDBWriter(
            con, "task_history", 7, bufsize=bufsize, overwrite=overwrite
        )
        self._task_suspend_meta = BufferedDBWriter(
            con, "task_suspend_meta", 4, bufsize=bufsize, overwrite=overwrite
        )

    def add_task_action(
        self,
        task: int,
        action: TaskAction,
        time: str,
        source_location: SourceLocation,
        location_ref: int,
        location_count: int,
        /,
    ) -> None:
        self._task_actions.insert(
            0,  # branch of history
            task,
            action,
            time,
            self._source_location_id[source_location],
            location_ref,
            location_count,
        )

    def add_task_suspend_meta(
        self, task: int, time: str, sync_descendants: bool
    ) -> None:
        self._task_suspend_meta.insert(
            0,  # branch of task meta history
            task,
            time,
            int(sync_descendants),
        )

    def close(self):
        self.debug("closing...")
        self._task_actions.close()
        self._task_suspend_meta.close()
