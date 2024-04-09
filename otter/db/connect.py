from __future__ import annotations

import os
import sqlite3
from functools import lru_cache
from typing import (
    Generator,
    List,
    Tuple,
    Union,
    Sequence,
    Callable,
)

import otter.log

from .types import (
    SourceLocation,
    TaskAttributes,
    Task,
    TaskSchedulingState,
)

from . import scripts


class Connection(sqlite3.Connection):
    """Implements the connection to and operations on an Otter task database"""

    def __init__(
        self, db: str, overwrite: bool = False, initialise: bool = False, **kwargs
    ):
        prefix = f"[{self.__class__.__name__}]"
        self.warning = otter.log.log_with_prefix(prefix, otter.log.warning)
        self.debug = otter.log.log_with_prefix(prefix, otter.log.debug)
        self.info = otter.log.log_with_prefix(prefix, otter.log.info)
        self.db = db
        self._callbacks_on_close: List[Callable] = []
        sqlite3_version = getattr(sqlite3, "sqlite_version", "???")
        otter.log.info(f"using sqlite3.sqlite_version {sqlite3_version}")
        if overwrite and os.path.exists(db):
            self.warning("overwriting tasks database %s", db)
            os.remove(db)
        super().__init__(db, **kwargs)
        if initialise:
            self.initialise()

    def initialise(self):
        self.info(" -- create tables")
        self.executescript(scripts.create_tables)
        self.info(" -- create indexes")
        self.executescript(scripts.create_indexes)
        self.info(" -- create views")
        self.executescript(scripts.create_views)
        return self

    def finalise(self):
        self.info(" -- update task locations and timestamps")
        self.executescript(scripts.update_task_locations_times)
        self.info(" -- count children")
        self.executescript(scripts.update_task_num_children)
        self.info(" -- update source location definitions")
        self.executescript(scripts.update_source_info)
        return self

    def commit(self):
        self.debug("commit called")
        super().commit()

    def on_close(self, callback: Callable):
        self._callbacks_on_close.append(callback)

    def close(self) -> None:
        self.debug(f"closing {self}")
        for callback in self._callbacks_on_close:
            self.debug(f"call {callback}")
            callback()
        super().close()

    def print_summary(self) -> None:
        """Print summary information about the connected tasks database"""

        row_format = "{0:<8s} {1:27s} {2:>6d}"

        otter.log.debug("try to read from sqlite_schema")
        query = "select name, type from {table} where type in ('table', 'view') order by type, name"
        rows: List[Tuple[str, str]]
        try:
            rows = self.execute(query.format(table="sqlite_schema")).fetchall()
        except sqlite3.OperationalError as err:
            otter.log.debug(err)
            otter.log.debug("failed to read from sqlite_schema, try from sqlite_master")
            rows = self.execute(query.format(table="sqlite_master")).fetchall()

        header = "Type     Name                          Rows"
        print(header)
        print("-" * len(header))
        for name, table_or_view in rows:
            query_count_rows = f"select count(*) from {name}"
            otter.log.debug(query_count_rows)
            (count,) = self.execute(query_count_rows).fetchone()
            print(row_format.format(table_or_view, name, count))

    def num_tasks(self) -> int:
        (count,) = self.execute("select count(*) from task").fetchone()
        return count

    def root_tasks(self) -> Tuple[int]:
        return (0,)

    def num_children(self, task: int) -> int:
        query = "select count(*) from task_relation where parent_id in (?)"
        (count,) = self.execute(query, (task,)).fetchone()
        return count

    def children_of(self, parent: int) -> List[int]:
        query = "select child_id from task_relation where parent_id in (?)"
        cur = self.execute(query, (parent,))
        return [task for (task,) in cur]

    def ancestors_of(self, task: int) -> List[int]:
        cur = self.execute(scripts.get_ancestors, (task,))
        return [task for (task,) in cur]

    def descendants_of(self, task: int) -> List[int]:
        cur = self.execute(scripts.get_descendants, (task,))
        return [task for (task,) in cur]

    def parent_child_attributes(
        self,
    ) -> List[Tuple[TaskAttributes, TaskAttributes, int]]:
        """Return tuples of task attributes for each parent-child link and the number of such links"""

        cur = self.execute(scripts.count_children_by_parent_attributes)
        results = [
            (TaskAttributes(*row[0:11]), TaskAttributes(*row[11:22]), row[22])
            for row in cur
        ]
        return results

    def _make_task(self, row) -> Task:
        """Make a task from its attributes and source location refs"""
        return Task(*row[0:7], *map(self.get_source_location, row[7:]))

    def get_tasks(self, tasks: Union[int, Sequence[int]]) -> List[Task]:
        if isinstance(tasks, int):
            tasks = (tasks,)
        placeholder = ",".join("?" for _ in tasks)
        query = scripts.get_task_attributes.format(placeholder=placeholder)
        return list(map(self._make_task, self.execute(query, tuple(tasks))))

    @property
    def tasks(self):
        """An iterator over all tasks in the db"""
        return map(self._make_task, self.execute(scripts.get_all_task_attributes))

    @lru_cache(maxsize=1000)
    def get_string(self, string_id: int) -> str:
        (string,) = self.execute(scripts.get_string, (string_id,)).fetchone()
        return string

    @lru_cache(maxsize=1000)
    def get_source_location(self, location_id: int) -> SourceLocation:
        """Construct a source location from its id"""
        row = self.execute(scripts.get_source_location, (location_id,)).fetchone()
        return SourceLocation(*row)

    def get_all_source_locations(self) -> List[Tuple[int, SourceLocation]]:
        """Get all the source locations defined in the trace"""

        results = [
            (location_id, SourceLocation(*row))
            for (location_id, *row) in self.execute(
                "select src_loc_id, file_name, func_name, line from source_location order by file_name, line"
            )
        ]
        return results

    def task_types(self) -> Generator[Tuple[TaskAttributes, int], None, None]:
        """Return task attributes for each distinct set of task attributes and the number of such records"""

        cur = self.execute(scripts.count_tasks_by_attributes)
        return ((TaskAttributes(row[0], -1, *row[1:10]), row[10]) for row in cur)

    def task_scheduling_states(self, tasks: Tuple[int]) -> List[TaskSchedulingState]:
        """Return 1 row per task scheduling state during the task's lifetime"""

        query = scripts.get_task_scheduling_states.format(
            placeholder=",".join("?" for task in tasks)
        )
        cur = self.execute(query, tasks)
        return [TaskSchedulingState(*row) for row in cur]

    def task_suspend_meta(self, task: int) -> Tuple[Tuple[str, bool], ...]:
        """Return the metadata for each suspend event encountered by a task"""

        query = "select time, sync_descendants from task_suspend_meta where id in (?)"
        cur = self.execute(query, (task,))
        return tuple((time, bool(sync_descendants)) for (time, sync_descendants) in cur)

    def children_created_between(
        self, task: int, start_ts: str, end_ts: str
    ) -> List[Tuple[int, str]]:
        """Return the children created between the given start & end times"""

        query = scripts.get_children_created_between.format(
            start_ts=start_ts, end_ts=end_ts
        )
        cur = self.execute(query, (task,))
        return list(cur)
