from typing import Optional, Sequence, Protocol, List, Dict, Tuple
from itertools import count

import otter.log
import otter.db
from otter.definitions import TaskAction


class ScheduleWriterProtocol(Protocol):

    def insert(self, task: int, action: TaskAction, event_ts: int, /, *args): ...


class CriticalTaskWriterProtocol(Protocol):

    def insert(self, task: int, sequence: int, critical_child: int, /, *args): ...


class TaskScheduler:

    def __init__(
        self,
        con: otter.db.Connection,
        schedule_writer: ScheduleWriterProtocol,
        crit_task_writer: CriticalTaskWriterProtocol,
        initial_tasks: Optional[Sequence[int]] = None,
    ) -> None:
        self.con = con
        self.schedule_writer = schedule_writer
        self.crit_task_writer = crit_task_writer
        self._root_tasks = initial_tasks or con.root_tasks()
        otter.log.debug("found %d root tasks", len(self._root_tasks))

    def run(self) -> None:
        max_root_task_dt = 0
        global_ts = 0
        root_task_attributes = self.con.task_attributes(self._root_tasks)
        otter.log.info("simulate %d tasks", self.con.num_tasks())
        for root_task_attr in root_task_attributes:
            print(f"Simulate root task {root_task_attr.id}")
            print(f"    Start: {root_task_attr.descriptor.start_location}")
            print(f"    End:   {root_task_attr.descriptor.end_location}")
            duration_observed = int(root_task_attr.end_ts) - int(
                root_task_attr.start_ts
            )
            duration = self.descend(
                root_task_attr.id,
                root_task_attr.start_ts,
                root_task_attr.end_ts,
                0,
                global_ts,
            )
            speedup = duration_observed / duration
            max_root_task_dt = max(max_root_task_dt, duration)
            print("    Duration:")
            print(f"      observed:  {duration_observed:>12d}")
            print(f"      simulated: {duration:>12d}")
            print(f"      speedup:   {speedup:>12.2f}")
            print(f"      relative:  {1/speedup:12.2%}")

    def descend(
        self, task: int, start_ts: str, end_ts: str, depth: int, global_start_ts: int
    ):
        """Descend into the children of task. At the root, handle leaf tasks"""
        # self.schedule_writer.insert task-start action
        self.schedule_writer.insert(task, TaskAction.START, global_start_ts)
        if self.con.num_children(task) > 0:
            duration = self.branch_task(task, start_ts, end_ts, depth, global_start_ts)
        else:
            duration = self.leaf_task(task, start_ts, end_ts, depth, global_start_ts)
        # self.schedule_writer.insert task-complete action
        self.schedule_writer.insert(task, TaskAction.END, global_start_ts + duration)
        return duration

    def branch_task(
        self,
        task: int,
        start_ts: str,
        end_ts: str,
        depth: int,
        global_start_ts: int,
    ):
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

        task_states = self.con.task_scheduling_states((task,))
        task_suspend_meta = dict(self.con.task_suspend_meta(task))
        children_pending: Dict[str, List[Tuple[int, str]]] = {}
        otter.log.debug("got %d task scheduling states", len(task_states))
        barrier_counter = count()
        for state in task_states:
            if state.action_start in (TaskAction.START, TaskAction.RESUME):
                # task in an active state
                execution_native_dt = execution_native_dt + state.duration
                # store the children created during this period
                children_pending[state.end_ts] = self.con.children_created_between(
                    task, state.start_ts, state.end_ts
                )
                global_start_ts = global_start_ts + state.duration
            elif state.action_start == TaskAction.SUSPEND:
                # task is suspended
                critical_task = None
                barrier_duration = 0
                sync_descendants = task_suspend_meta[state.start_ts]
                # get the children synchronised at this point
                children = children_pending.get(state.start_ts, [])
                child_ids: List[int]
                child_ids, _ = list(zip(*children))
                children_attr = self.con.task_attributes(child_ids)
                for child_attr in children_attr:
                    child_crt_dt = int(state.start_ts) - int(child_attr.create_ts)
                    child_duration = self.descend(
                        child_attr.id,
                        child_attr.start_ts,
                        child_attr.end_ts,
                        depth + 1,
                        global_start_ts - child_crt_dt,
                    )
                    duration_into_barrier = child_duration - (
                        int(state.start_ts) - int(child_attr.create_ts)
                    )
                    if duration_into_barrier > barrier_duration:
                        barrier_duration = duration_into_barrier
                        critical_task = child_attr.id
                if critical_task is not None:
                    self.crit_task_writer.insert(
                        task, next(barrier_counter), critical_task
                    )
                suspended_ideal_dt = suspended_ideal_dt + barrier_duration
                global_start_ts = global_start_ts + barrier_duration
            elif state.action_start == TaskAction.CREATE:
                # task is created and not yet started
                pass
            else:
                otter.log.error("UNKNOWN STATE: %s", state)

        return execution_native_dt + suspended_ideal_dt

    def leaf_task(
        self,
        task: int,
        start_ts: str,
        end_ts: str,
        depth: int,
        global_start_ts: int,
    ):
        # Returns the duration of a leaf task. Assumes a leaf task is executed in one go i.e. never suspended.
        pre = "+" * depth
        start = int(start_ts)
        end = int(end_ts)
        duration = end - start
        return duration


def simulate_ideal(con: otter.db.Connection):
    schedule_writer = otter.db.ScheduleWriter(con)
    crit_task_writer = otter.db.CritTaskWriter(con)
    con.on_close(schedule_writer.close)
    con.on_close(crit_task_writer.close)
    scheduler = TaskScheduler(con, schedule_writer, crit_task_writer)
    scheduler.run()
