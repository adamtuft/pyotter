from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Generator, List, Tuple, Sequence

from .connect_base import Mode, ConnectionBase
from .scripts import scripts
from .types import (
    SourceLocation,
    TaskAttributes,
    Task,
    TaskSchedulingState,
)


class ReadConnection(ConnectionBase):
    """Implements all logic for querying an otter database"""

    #! TODO: add queries for listing simulations, getting sim task history, getting event positions

    def __init__(self, root_path: Path) -> None:
        super().__init__(root_path, mode=Mode.ro)

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex, tb):
        pass

    def count_rows(self) -> List[Tuple[str, str, int]]:

        self.log_debug("try to read from sqlite_master")
        rows = self._con.execute(scripts["select_names_from_sqlite_master"]).fetchall()

        counts: List[Tuple[str, str, int]]
        counts = [
            (
                table_or_view,
                name,
                self._con.execute(f"select count(*) from {name}").fetchone()[0],
            )
            for (table_or_view, name) in rows
        ]
        return counts

    def count_tasks(self) -> int:
        (count,) = self._con.execute("select count(*) from task").fetchone()
        return count

    def count_simulations(self) -> int:
        (count,) = self._con.execute(
            "select count(distinct sim_id) from sim_task_history"
        ).fetchone()
        return count

    def count_simulation_rows(self) -> List[Tuple[int, int]]:
        cur = self._con.execute(scripts["count_simulation_rows"]).fetchall()
        return list(cur)

    def get_root_tasks(self) -> Tuple[int]:
        return (0,)

    def get_num_children(self, task: int) -> int:
        query = "select count(*) from task_relation where parent_id in (?)"
        (count,) = self._con.execute(query, (task,)).fetchone()
        return count

    def get_children_of(self, parent: int) -> List[int]:
        query = "select child_id from task_relation where parent_id in (?)"
        cur = self._con.execute(query, (parent,))
        return [task for (task,) in cur]

    def get_ancestors_of(self, task: int) -> List[int]:
        cur = self._con.execute(scripts["get_ancestors"], (task,))
        return [task for (task,) in cur]

    def get_descendants_of(self, task: int) -> List[int]:
        cur = self._con.execute(scripts["get_descendants"], (task,))
        return [task for (task,) in cur]

    def get_all_parent_child_attributes(
        self,
    ) -> List[Tuple[TaskAttributes, TaskAttributes, int]]:
        """Return tuples of task attributes for each parent-child link and the number of such links"""

        cur = self._con.execute(scripts["count_children_by_parent_attributes"])
        results = [
            (self._make_task_attr(*row[0:4]), self._make_task_attr(*row[4:8]), row[8])
            for row in cur
        ]
        return results

    def get_tasks(self, tasks: Sequence[int]) -> List[Task]:
        placeholder = ",".join("?" for _ in tasks)
        query = scripts["get_task_attributes"].format(placeholder=placeholder)
        return list(map(self._make_task, self._con.execute(query, tuple(tasks))))

    def iter_all_tasks(self):
        """An iterator over all tasks in the db"""
        return map(self._make_task, self._con.execute(scripts["get_all_task_attributes"]))

    @lru_cache(maxsize=1000)
    def get_string(self, string_id: int) -> str:
        (string,) = self._con.execute(scripts["get_string"], (string_id,)).fetchone()
        return string

    @lru_cache(maxsize=1000)
    def get_source_location(self, location_id: int) -> SourceLocation:
        """Construct a source location from its id"""
        row = self._con.execute(scripts["get_source_location"], (location_id,)).fetchone()
        return SourceLocation(*row)

    def get_all_source_locations(self) -> List[Tuple[int, SourceLocation]]:
        """Get all the source locations defined in the trace"""

        results = [
            (location_id, SourceLocation(*row))
            for (location_id, *row) in self._con.execute(
                "select src_loc_id, file_name, func_name, line from source_location order by file_name, line"
            )
        ]
        return results

    def get_all_strings(self) -> List[Tuple[int, str]]:
        return list(self._con.execute("select id, text from string order by id;"))

    def iter_all_task_types(self) -> Generator[Tuple[TaskAttributes, int], None, None]:
        """Return task attributes for each distinct set of task attributes and the number of such records"""

        cur = self._con.execute(scripts["count_tasks_by_attributes"])
        return ((self._make_task_attr(*row[0:4]), row[4]) for row in cur)

    def get_task_scheduling_states(self, tasks: Tuple[int]) -> List[TaskSchedulingState]:
        """Return 1 row per task scheduling state during the task's lifetime"""

        query = scripts["get_task_scheduling_states"].format(
            placeholder=",".join("?" for task in tasks)
        )
        cur = self._con.execute(query, tasks)
        return [TaskSchedulingState(*row) for row in cur]

    def get_task_event_positions(self, task: int) -> List[Tuple[int, int]]:
        return list(self._con.execute(scripts["get_task_events"], (task,)))

    def get_task_suspend_meta(self, task: int) -> Tuple[Tuple[str, bool], ...]:
        """Return the metadata for each suspend event encountered by a task"""

        query = "select time, sync_descendants from task_suspend_meta where id in (?)"
        cur = self._con.execute(query, (task,))
        return tuple((time, bool(sync_descendants)) for (time, sync_descendants) in cur)

    def get_children_created_between(
        self, task: int, start_ts: str, end_ts: str
    ) -> List[Tuple[int, str]]:
        """Return the children created between the given start & end times"""

        query = scripts["get_children_created_between"].format(start_ts=start_ts, end_ts=end_ts)
        cur = self._con.execute(query, (task,))
        return list(cur)

    # Row factories

    def _make_task(self, row) -> Task:
        """Make a task from its attributes and source location refs"""
        return Task(*row[0:7], *map(self.get_source_location, row[7:]))

    def _make_task_attr(self, label: str, create: int, start: int, end: int) -> TaskAttributes:
        return TaskAttributes(
            label,
            self.get_source_location(create),
            self.get_source_location(start),
            self.get_source_location(end),
        )