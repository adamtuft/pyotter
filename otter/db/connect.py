from __future__ import annotations

import os
import sqlite3
from abc import ABC, abstractmethod
from contextlib import ExitStack
from enum import Enum, auto
from functools import lru_cache
from pathlib import Path
from typing import Generator, List, Tuple, Union, Sequence, Callable, Dict

import otter.log

from otter.log import Loggable
from otter.utils import LabellingDict

from .scripts import scripts
from .types import (
    SourceLocation,
    TaskAttributes,
    Task,
    TaskSchedulingState,
)
from .writers import TaskActionWriter, TaskMetaWriter, SourceLocationWriter, StringDefinitionWriter


class Mode(Enum):
    READ = auto()
    WRITE = auto()
    SIMULATE = auto()


class Connection(sqlite3.Connection):
    """Implements the connection to and operations on an Otter task database"""

    def __init__(self, db: str, overwrite: bool = False, initialise: bool = False, **kwargs):
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

    def initialise(self, /, *, views: bool = False):
        self.info(" -- create tables")
        self.executescript(scripts["create_tables"])
        self.info(" -- create indexes")
        self.executescript(scripts["create_indexes"])
        if views:
            self.info(" -- create views")
            self.executescript(scripts["create_views"])
        return self

    def finalise(self):
        self.info(" -- update task locations and timestamps")
        self.executescript(scripts["update_task_locations_times"])
        self.info(" -- count children")
        self.executescript(scripts["update_task_num_children"])
        self.info(" -- update source location definitions")
        self.executescript(scripts["update_source_info"])
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

    def count_rows(self) -> List[Tuple[str, str, int]]:

        otter.log.debug("try to read from sqlite_master")
        rows = self.execute(scripts["select_names_from_sqlite_master"]).fetchall()

        counts: List[Tuple[str, str, int]]
        counts = [
            (
                table_or_view,
                name,
                self.execute(f"select count(*) from {name}").fetchone()[0],
            )
            for (table_or_view, name) in rows
        ]
        return counts

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
        cur = self.execute(scripts["get_ancestors"], (task,))
        return [task for (task,) in cur]

    def descendants_of(self, task: int) -> List[int]:
        cur = self.execute(scripts["get_descendants"], (task,))
        return [task for (task,) in cur]

    def _make_task_attr(self, label: str, create: int, start: int, end: int) -> TaskAttributes:
        return TaskAttributes(
            label,
            self.get_source_location(create),
            self.get_source_location(start),
            self.get_source_location(end),
        )

    def parent_child_attributes(
        self,
    ) -> List[Tuple[TaskAttributes, TaskAttributes, int]]:
        """Return tuples of task attributes for each parent-child link and the number of such links"""

        cur = self.execute(scripts["count_children_by_parent_attributes"])
        results = [
            (self._make_task_attr(*row[0:4]), self._make_task_attr(*row[4:8]), row[8])
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
        query = scripts["get_task_attributes"].format(placeholder=placeholder)
        return list(map(self._make_task, self.execute(query, tuple(tasks))))

    @property
    def tasks(self):
        """An iterator over all tasks in the db"""
        return map(self._make_task, self.execute(scripts["get_all_task_attributes"]))

    @lru_cache(maxsize=1000)
    def get_string(self, string_id: int) -> str:
        (string,) = self.execute(scripts["get_string"], (string_id,)).fetchone()
        return string

    @lru_cache(maxsize=1000)
    def get_source_location(self, location_id: int) -> SourceLocation:
        """Construct a source location from its id"""
        row = self.execute(scripts["get_source_location"], (location_id,)).fetchone()
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

    def get_all_strings(self) -> List[Tuple[int, str]]:
        return list(self.execute("select id, text from string order by id;"))

    def task_types(self) -> Generator[Tuple[TaskAttributes, int], None, None]:
        """Return task attributes for each distinct set of task attributes and the number of such records"""

        cur = self.execute(scripts["count_tasks_by_attributes"])
        return ((self._make_task_attr(*row[0:4]), row[4]) for row in cur)

    def task_scheduling_states(self, tasks: Tuple[int]) -> List[TaskSchedulingState]:
        """Return 1 row per task scheduling state during the task's lifetime"""

        query = scripts["get_task_scheduling_states"].format(
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

        query = scripts["get_children_created_between"].format(start_ts=start_ts, end_ts=end_ts)
        cur = self.execute(query, (task,))
        return list(cur)

    def print_row_count(self, sep: str = " "):
        for items in self.count_rows():
            print(*items, sep=sep)

    def print_source_locations(self, sep: str = " "):
        for location_id, location in self.get_all_source_locations():
            print(location_id, location.file, location.line, location.func, sep=sep)

    def print_strings(self, sep: str = " "):
        for ref, string in self.get_all_strings():
            print(ref, string, sep=sep)

    def print_tasks(self, sep: str = " "):
        for desc, num_tasks in self.task_types():
            print(
                num_tasks,
                desc.label,
                f"{desc.create_location.file}:{desc.create_location.line}",
                f"{desc.start_location.file}:{desc.start_location.line}",
                f"{desc.end_location.file}:{desc.end_location.line}",
                sep=sep,
            )


class ConnectionBase(ABC, Loggable):

    @abstractmethod
    def __enter__(self): ...

    @abstractmethod
    def __exit__(self, ex_type, ex, tb): ...


class ReadConnection(ConnectionBase):
    pass


class WriteConnection(ConnectionBase):

    def __init__(self, root_path: Path, /, *, views: bool = False) -> None:
        self.views = views
        dbpath = root_path / "aux" / "tasks.db"
        if dbpath.exists():
            self.log_warning("overwriting tasks database %s", dbpath)
            dbpath.unlink()
        self._con = sqlite3.connect(dbpath)
        source_location_id: Dict[SourceLocation, int] = LabellingDict()
        string_id: Dict[str, int] = LabellingDict()
        self._task_meta_writer = TaskMetaWriter(self._con, string_id, bufsize=1000000)
        self._task_action_writer = TaskActionWriter(self._con, source_location_id, bufsize=1000000)
        self._exit = ExitStack()
        # Must push string writer before source writer so all strings have been seen when string writer closed
        self._exit.enter_context(StringDefinitionWriter(self._con, string_id))
        self._exit.enter_context(SourceLocationWriter(self._con, string_id, source_location_id))
        self._exit.enter_context(self._task_meta_writer)
        self._exit.enter_context(self._task_action_writer)

    def __enter__(self):
        self.log_info(" -- create tables")
        self._con.executescript(scripts["create_tables"])
        self.log_info(" -- create indexes")
        self._con.executescript(scripts["create_indexes"])
        if self.views:
            self.log_info(" -- create views")
            self._con.executescript(scripts["create_views"])
        return (
            self._task_meta_writer.add_task_metadata,
            self._task_action_writer.add_task_action,
            self._task_action_writer.add_task_suspend_meta,
        )

    def __exit__(self, ex_type, ex, tb):
        if ex_type is None:
            self.log_info(" -- close writers")
            self._exit.close()
            self.log_info(" -- update task locations and timestamps")
            self._con.executescript(scripts["update_task_locations_times"])
            self.log_info(" -- count children")
            self._con.executescript(scripts["update_task_num_children"])
            self.log_info(" -- update source location definitions")
            self._con.executescript(scripts["update_source_info"])
            return True
        else:
            self.log_error("database not finalised due to unhandled exception")
            return False
