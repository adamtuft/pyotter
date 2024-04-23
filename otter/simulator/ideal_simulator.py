from typing import Optional, Sequence, Protocol, List, Dict, Tuple
from itertools import count

import otter.log
import otter.db

from otter.log import Loggable
from otter.db import ReadConnection
from otter.db.protocols import TaskActionCallback, TaskSuspendMetaCallback, CriticalTaskCallback
from otter.db.types import Task
from otter.definitions import TaskAction


class TaskScheduler(Loggable):

    def __init__(
        self,
        con: ReadConnection,
        crit_task_callback: CriticalTaskCallback,
        task_action_callback: TaskActionCallback,
        task_suspend_callback: TaskSuspendMetaCallback,
        initial_tasks: Optional[Sequence[int]] = None,
    ) -> None:
        self.log_debug("CALLBACKS:")
        self.log_debug("CALLBACKS: %s", crit_task_callback)
        self.log_debug("CALLBACKS: %s", task_action_callback)
        self.log_debug("CALLBACKS: %s", task_suspend_callback)
        self.con = con
        self.crit_task_callback = crit_task_callback
        self.task_action_callback = task_action_callback
        self.task_suspend_callback = task_suspend_callback
        self._root_tasks = initial_tasks or con.get_root_tasks()
        otter.log.debug("found %d root tasks", len(self._root_tasks))

    def run(self) -> None:
        max_root_task_dt = 0
        global_ts = 0
        root_task_attributes = self.con.get_tasks(self._root_tasks)
        otter.log.info("simulate %d tasks", self.con.count_tasks())
        for root_task in root_task_attributes:
            print(f"Simulate root task {root_task.id}")
            print(f"    Start: {root_task.attr.start_location}")
            print(f"    End:   {root_task.attr.end_location}")
            duration_observed = int(root_task.end_ts) - int(root_task.start_ts)
            duration = self.descend(root_task, 0, global_ts)
            speedup = duration_observed / duration
            max_root_task_dt = max(max_root_task_dt, duration)
            print("    Duration:")
            print(f"      observed:  {duration_observed:>12d}")
            print(f"      simulated: {duration:>12d}")
            print(f"      speedup:   {speedup:>12.2f}")
            print(f"      relative:  {1/speedup:12.2%}")

    def descend(self, task: Task, depth: int, global_start_ts: int):
        """Descend into the children of task. At the root, handle leaf tasks"""
        self.task_action_callback(
            task.id, TaskAction.START, str(global_start_ts), task.attr.start_location
        )
        if task.children > 0:
            duration = self.branch_task(task, depth, global_start_ts)
        else:
            duration = self.leaf_task(task, depth, global_start_ts)
        self.task_action_callback(
            task.id, TaskAction.END, str(global_start_ts + duration), task.attr.end_location
        )
        return duration

    def branch_task(self, task: Task, depth: int, global_start_ts: int):
        """Returns duration elapsed from task-start to task-end, including time spent
        waiting for children/descendants, but not including the duration of any
        children which are not synchronised

        Think of this as the "taskwait-inclusive duration", which is not the same as
        the "inclusive duration" i.e. this task + all descendants.add

        The taskwait-inclusive duration is composed of two parts:
        - time spent executing the task itself (recorded in the trace)
        - time in which the task is suspended at a barrier (but modelled for an
        infinite machine)

        In our idealised infinite scheduler, we get execution time directly from
        the trace (start_ts, suspended_ts, resume_ts and end_ts).

        However, the suspended duration as recorded depends on the number of tasks
        that could be executed in parallel. For our infinite machine, this is
        unlimited so the idealised suspended duration should just be the maximum
        "taskwait-inclusive duration" among the children synchronised by a barrier
        """

        execution_native_dt, suspended_ideal_dt = 0, 0

        task_states = self.con.get_task_scheduling_states((task.id,))
        task_suspend_meta = dict(self.con.get_task_suspend_meta(task.id))
        children_pending: Dict[str, List[Tuple[int, str]]] = {}
        otter.log.debug("got %d task scheduling states", len(task_states))
        barrier_counter = count()
        for state in task_states:
            if state.action_start in (TaskAction.START, TaskAction.RESUME):
                # task in an active state
                execution_native_dt = execution_native_dt + state.duration
                # store the children created during this period
                children_pending[state.end_ts] = self.con.get_children_created_between(
                    task.id, state.start_ts, state.end_ts
                )
                global_start_ts = global_start_ts + state.duration
            elif state.action_start == TaskAction.SUSPEND:
                # task is suspended
                self.task_action_callback(
                    task.id, TaskAction.SUSPEND, str(global_start_ts), state.start_location
                )
                critical_task = None
                barrier_duration = 0
                sync_descendants = task_suspend_meta[state.start_ts]
                self.task_suspend_callback(task.id, state.start_ts, sync_descendants)
                # get the children synchronised at this point
                children = children_pending.get(state.start_ts, [])
                child_ids: List[int]
                child_ids, _ = list(zip(*children))
                children_attr = self.con.get_tasks(child_ids)
                for child in children_attr:
                    child_crt_dt = int(state.start_ts) - int(child.create_ts)
                    child_duration = self.descend(
                        child,
                        depth + 1,
                        global_start_ts - child_crt_dt,
                    )
                    duration_into_barrier = child_duration - (
                        int(state.start_ts) - int(child.create_ts)
                    )
                    if duration_into_barrier > barrier_duration:
                        barrier_duration = duration_into_barrier
                        critical_task = child.id
                if critical_task is not None:
                    self.crit_task_callback(task.id, next(barrier_counter), critical_task)
                suspended_ideal_dt = suspended_ideal_dt + barrier_duration
                global_start_ts = global_start_ts + barrier_duration
                self.task_action_callback(
                    task.id, TaskAction.RESUME, str(global_start_ts), state.end_location
                )
            elif state.action_start == TaskAction.CREATE:
                # task is created and not yet started
                pass
            else:
                otter.log.error("UNKNOWN STATE: %s", state)

        return execution_native_dt + suspended_ideal_dt

    def leaf_task(self, task: Task, depth: int, global_start_ts: int):
        # Returns the duration of a leaf task. Assumes a leaf task is executed in one go i.e. never suspended.
        pre = "+" * depth
        start = int(task.start_ts)
        end = int(task.end_ts)
        duration = end - start
        return duration


def simulate_ideal(
    reader: ReadConnection,
    crit_task_callback: CriticalTaskCallback,
    task_action_callback: TaskActionCallback,
    task_suspend_callback: TaskSuspendMetaCallback,
):
    TaskScheduler(reader, crit_task_callback, task_action_callback, task_suspend_callback).run()
