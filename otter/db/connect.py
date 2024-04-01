from __future__ import annotations

import sqlite3
from typing import (
    Generator,
    Iterable,
    List,
    Tuple,
    Union,
    Sequence,
    Callable,
)

import otter.log
from ..definitions import (
    SourceLocation,
    TaskDescriptor,
    TaskAttributes,
    TaskSchedulingState,
)
from . import scripts


class Connection(sqlite3.Connection):
    """Implements the connection to and operations on an Otter task database"""

    def __init__(self, db: str, **kwargs):
        super().__init__(db, **kwargs)
        prefix = f"[{self.__class__.__name__}]"
        self.debug = otter.log.log_with_prefix(prefix, otter.log.debug)
        self.info = otter.log.log_with_prefix(prefix, otter.log.info)
        self.db = db
        self._callbacks_on_close: List[Callable] = []
        sqlite3_version = getattr(sqlite3, "sqlite_version", "???")
        otter.log.info(f"using sqlite3.sqlite_version {sqlite3_version}")

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
        cur = self.cursor()
        cur.execute("select count(*) as num_tasks from task")
        return cur.fetchone()["num_tasks"]

    def task_ids(self) -> Iterable[int]:
        cur = self.cursor()
        cur.execute("select id from task order by rowid")
        while True:
            row = cur.fetchone()
            if row is None:
                break
            yield row["id"]

    def root_tasks(self) -> Tuple[int]:
        return (0,)

    def num_children(self, task: int) -> int:
        cur = self.cursor()
        query = (
            "select count(*) as num_children from task_relation where parent_id in (?)"
        )
        return cur.execute(query, (task,)).fetchone()[0]

    def children_of(self, parent: int) -> List[int]:
        cur = self.cursor()
        query = "select child_id from task_relation where parent_id in (?)"
        return [r["child_id"] for r in cur.execute(query, (parent,)).fetchall()]

    def ancestors_of(self, task: int) -> List[int]:
        cur = self.cursor()
        cur.execute(scripts.get_ancestors, (task,))
        return [row["id"] for row in cur.fetchall()]

    def descendants_of(self, task: int) -> List[int]:
        cur = self.cursor()
        cur.execute(scripts.get_descendants, (task,))
        return [row["id"] for row in cur.fetchall()]

    def task_attributes(self, tasks: Union[int, Sequence[int]]) -> List[TaskAttributes]:
        if isinstance(tasks, int):
            tasks = (tasks,)
        placeholder = ",".join("?" for _ in tasks)
        query_str = (
            f"select * from task_attributes where id in ({placeholder}) order by id\n"
        )
        cur = self.execute(query_str, tuple(tasks))
        return list(TaskAttributes(*row) for row in cur)

    def parent_child_attributes(
        self,
    ) -> List[Tuple[TaskDescriptor, TaskDescriptor, int]]:
        """Return tuples of task attributes for each parent-child link and the number of such links"""

        cur = self.execute(scripts.count_children_by_parent_attributes)
        results = [
            (TaskDescriptor(*row[0:11]), TaskDescriptor(*row[11:22]), row[22])
            for row in cur
        ]
        otter.log.debug("got %d rows", len(results))
        return results

    def source_locations(self) -> List[Tuple[int, SourceLocation]]:
        """Get all the source locations defined in the trace"""

        results = [
            (location_id, SourceLocation(*row))
            for (location_id, *row) in self.execute(
                "select src_loc_id, file_name, func_name, line from source_location order by file_name, line"
            )
        ]
        otter.log.debug("got %d source locations", len(results))
        return results

    def task_types(self) -> Generator[Tuple[TaskDescriptor, int], None, None]:
        """Return task attributes for each distinct set of task attributes and the number of such records"""

        cur = self.execute(scripts.count_tasks_by_attributes)
        return ((TaskDescriptor(row[0], -1, *row[1:10]), row[10]) for row in cur)

    def task_scheduling_states(self, tasks: Tuple[int]) -> List[TaskSchedulingState]:
        """Return 1 row per task scheduling state during the task's lifetime"""

        query = scripts.get_task_scheduling_states.format(
            placeholder=",".join("?" for task in tasks)
        )
        return [TaskSchedulingState(*row) for row in self.execute(query, tasks)]

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
        return list(self.execute(query, (task,)))
