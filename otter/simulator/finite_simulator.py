import argparse
from collections import deque
from contextlib import ExitStack
from enum import Enum, auto
from typing import Deque, Dict, List, Literal, Optional, Set, Tuple, Union

import otf2_ext

import otter
from otter import log
from otter.core.chunk_reader import ChunkReaderProtocol, DBChunkReader
from otter.definitions import EventType


class TSP(Enum):
    """Represents the set of modelled task-scheduling points"""

    CREATE = auto()
    SUSPEND = auto()
    COMPLETE = auto()


TaskSchedulingPoint = Union[
    Tuple[
        Literal[TSP.CREATE], int, int, int, int
    ],  # (TSP.CREATE, time, thread, task, child_task)
    Tuple[Literal[TSP.SUSPEND], int, int, int],  # (TSP.SUSPEND, time, thread, task)
    Tuple[
        Literal[TSP.COMPLETE], int, int, int, int
    ],  # (TSP.COMPLETE, time, thread, task, parent_task)
]

"""
Each TSP occurs as part of a segment of task. These segments start with either
a task-enter or task-resume event and end at the very next task-suspend or task-leave
event.

For example, this task chunk:

task-enter
task-create
task-sync-enter
task-sync-leave
task-create
task-create
task-create
task-sync-enter
task-sync-leave
task-leave

Has these task segments:

1:
    task-enter
    task-create
    task-sync-enter

2:
    task-sync-leave
    task-create
    task-create
    task-create
    task-sync-enter

3:
    task-sync-leave
    task-leave

Each TSP occurs at a known duration into the task segment.
"""


class TaskPool:
    """Encapsulates the connection to the tasks database, responsible for
    traversing the database to spawn tasks

    If no set of initial tasks waiting to be scheduled is given, take these from
    `con`.

    When a task is scheduled, generates data for the task-scheduling points
    the task encounters up to & including a task-suspend or task-complete TSP.
    The timestamps of these TSPs are given as offsets from the task start ts.
    """

    def __init__(
        self,
        con: otter.db.Connection,
        chunk_reader: ChunkReaderProtocol,
        initial_tasks: Optional[Set[int]] = None,
    ) -> None:
        self.con = con
        self.chunk_reader = chunk_reader

        # The set of tasks ready to be scheduled on a thread i.e. with no outstanding dependencies
        self._ready_tasks = initial_tasks or set(con.root_tasks())

        # The set of suspended tasks with a count of outstanding dependencies
        self._waiting_tasks: Dict[int, int] = {}

        # The set of currently running tasks - used to track which task chunk we're up to
        # Tasks in here are those which have multiple chunks to schedule (because they contain at least 1 task-sync point)
        # Tasks are added here the first time they are scheduled
        # Tasks remain in here until they are completed at which point they are removed
        self._running_tasks: Dict[int, int] = {}

        #! Note: the set of all tasks currently suspended is self._ready_tasks + self._waiting_tasks

    def get_ready_tasks(self):
        return self._ready_tasks.copy()

    def tasks_pending(self):
        return bool(self._ready_tasks)

    def schedule_new_task(self, task: int, thread: int):
        """Start a new task. This requires that the task is in the set of ready
        tasks and is not in the set of running tasks. Raise an error if this isn't
        true.

        Remove the given task from the set of ready tasks

        Return data sufficient to construct the TSPs that result from scheduling
        this task (& in particular this task chunk)
        """

        msg = None
        if task in self._running_tasks:
            msg = f"task {task} already in set of running tasks"
        elif task not in self._ready_tasks:
            msg = f"task {task} not in set of ready tasks"

        if msg:
            log.error(msg)
            raise ValueError(msg)

        self._ready_tasks.remove(task)

        task, parent, num_children, start_ts, end_ts, attr = self.con.task_attributes(
            task
        )[0]

        if num_children == 0:
            # If the given task is a leaf task, the only TSP data is from the task-complete event
            tsp_data = [(TSP.COMPLETE, int(start_ts), int(end_ts))]
        else:
            # If a task has any children, construct the sequence of TSPs it encounters
            tsp_data = []
            chunk = self.chunk_reader.get_chunk(task)
            assert chunk.first is not None
            assert chunk.first.event_type == EventType.task_enter
            for event in chunk.events:
                if event.event_type == EventType.task_create:
                    tsp_data.append(
                        (
                            TSP.CREATE,
                            event.time,
                            thread,
                            event.encountering_task_id,
                            event.unique_id,
                        )
                    )
                elif event.event_type == EventType.sync_begin:
                    tsp_data.append(
                        (
                            TSP.SUSPEND,
                            event.time,
                            thread,
                            event.encountering_task_id,
                        )
                    )
                elif event.event_type == EventType.task_leave:
                    tsp_data.append(
                        (
                            TSP.SUSPEND,
                            event.time,
                            thread,
                            event.encountering_task_id,
                        )
                    )

            # Return data sufficient to construct the TSPs of this task
            return tsp_data

        return tsp_data

    def task_created(self, task: int):
        """Add a new task to the pool of tasks ready to be scheduled"""
        assert not (task in self._ready_tasks or task in self._waiting_tasks)
        self._ready_tasks.add(task)

    def count_outstanding_children(self, task: int):
        pending_children = sum(
            1
            for child in self.con.children_of(task)
            if (
                child in self._ready_tasks
                or child in self._waiting_tasks
                or child in self._running_tasks
            )
        )
        return pending_children

    def notify_task_ready(self, task: int):
        """Record that this task is now available to be scheduled i.e. there are
        no outstanding dependencies."""
        assert not (task in self._ready_tasks or task in self._waiting_tasks)
        self._ready_tasks.add(task)

    def notify_task_waiting(self, task: int, num_dependencies: int):
        """Record that this task is not available to be scheduled until its
        outstanding dependencies are met"""
        assert not (task in self._ready_tasks or task in self._waiting_tasks)
        self._waiting_tasks[task] = num_dependencies

    def notify_task_complete(self, task: int, parent_task: int):
        """Record that a task was completed and notify any waiting parent"""
        if task in self._running_tasks:
            del self._running_tasks[task]
        if parent_task in self._waiting_tasks:
            self._waiting_tasks[parent_task] -= 1
            if self._waiting_tasks[parent_task] == 0:
                del self._waiting_tasks[parent_task]
                self.notify_task_ready(parent_task)
        assert not (task in self._ready_tasks or task in self._waiting_tasks)


class ThreadAgent:
    """A thread which can request and execute work"""

    def __init__(self, thread_id: int, scheduler: "TaskScheduler") -> None:
        self.id = thread_id
        self.scheduler = scheduler
        self._current_task_duration = 0
        self._current_task_start_ts = 0

    def activate(self):
        """Activate this thread. If not busy, request work from the scheduler.
        Consume the task for a given duration. Note the time at which the thread
        is next available to be activated."""
        print(f"[thread={self.id}] thread activated")

    def notify_next_available_ts(self):
        self.scheduler.set_next_available_ts(
            self.id, self._current_task_start_ts + self._current_task_duration
        )


class TaskScheduler:
    """Manages the task pool.

    Resolves TSPs in the global order in which they are spawned by the scheduled
    tasks. At each TSP, update the global time and fire the requisite handler
    for the type of TSP. Add any new TSPs to the queue and maintain the correct
    TSP order.

    At a given TSP, each available thread has the opportunity to make a task-
    scheduling decision.

    Possible TSPs are:
        - CREATE
        - SUSPEND
        - COMPLETE

    Possible choices are:
        - schedule a new tied/untied task
        - resume a suspended tied/untied task

    The choice made must satisfy the task-scheduling constraints for the particular
    thread
    """

    class Action(Enum):
        """Represents the actions that can be taken at a task-scheduling point"""

        START = auto()  # start a new task to execute on a thread
        RESUME = auto()  # resume a previously suspended task
        NONE = auto()  # take no action

    def __init__(
        self, task_pool: TaskPool, num_threads: int = 1, global_clock: int = 0
    ) -> None:
        self.task_pool = task_pool
        self.global_clock = global_clock

        # Each thread will send its next-available timestamp here
        self.next_available_ts = [0] * num_threads

        # The time-ordered list of task-scheduling points to be evaluated
        self._task_scheduling_points: Deque[TaskSchedulingPoint] = deque()

        self.threads = [ThreadAgent(n, self) for n in range(num_threads)]

    def step(self):
        """
        Get the time each thread is next available.
        Advance the global clock to the minimum such time.
        Activate all threads now available.

        What are the possible effects of scheduling a task on a thread?
            - Child tasks are spawned, and may be synchronised at a barrier.
                (tasks can have distinct creation & execution times!)
            - The thread is busy for the duration of the task i.e. unavailable
                for scheduling another task.

        When a task (chunk) is scheduled on a thread we can immediately get:
            - the crt_ts of tasks created during that task chunk
            - the id of tasks created during that task chunk
            - the duration of that task chunk i.e. the time the thread is busy
            - whether a task-sync occurs at the end of this task chunk

        When are the possible times we could then make a scheduling decision?
            - at task-create (i.e. could choose to schedule immediately on an available thread, or suspend the current task and schedule immediately on the current thread, or just add to task pool)
            - at task-sync-enter i.e. when a task is suspended (i.e. can choose to take any available task)
            - at task-complete i.e. when a task is completed (i.e. must notify task pool that the task is completed, and can then choose any available task)

        ## OpenMP 5.1 sec. 2.12.6 Task Scheduling:

        At a task-scheduling point, any of the following are permitted by OpenMP (subject to certain constraints):
            - start a new (tied or untied) task
            - resume any suspended (tied or untied) task

        Those constraints are:
            1. Scheduling of new tied tasks is constrained by the set of {task regions currently tied to the thread and not suspended in a barrier region}.
            - If there are any {task regions currently tied to the thread and not suspended in a barrier region}:
                - a new tied task may be scheduled on this thread only if it is a descendant of every task in the set.
            - Otherwise:
                - any new tied task may be scheduled on this thread.

            ==> this constraint basically enforces a depth-first scheduling of descendant tied tasks!!

            2. Task dependences must be satisfied

            3. Can't overlap mutually exclusive tasks

            4. Tasks with a false `if` clause are executed immediately after task-creation

        2 ways to process the effects of scheduling a task on a thread:
            1. immediately add effects to a queue of some sort
            2. resolve effects when task next suspended/completed.

        ==> Most sensible approach seems to be to replay the task-scheduling points that would be encountered in native execution, in order, making sure the set of ready/suspended tasks is kept up to date between TSPs.

        """
        mode, *data = self._task_scheduling_points.popleft()
        if mode == TSP.CREATE:
            # handle task-create TSP
            assert len(data) == 4
            global_ts, thread, task, child_task = data

            # 1. If eligible, add created task to set of ready tasks so it is
            # immediately ready to schedule
            self.task_pool.task_created(child_task)

        elif mode == TSP.SUSPEND:
            # handle task-suspend TSP i.e. encountered a taskwait
            assert len(data) == 3
            global_ts, thread, task = data

            # 1. Determine how many OUTSTANDING dependencies this task has
            #   - look at child tasks created but not completed
            num_dependencies = self.task_pool.count_outstanding_children(task)

            # 2. If 0 outstanding, add this task to the set of ready tasks
            if num_dependencies == 0:
                self.task_pool.notify_task_ready(task)

            # 3. Otherwise, add this task to the pool of waiting task with count
            # of unmet dependencies
            else:
                self.task_pool.notify_task_waiting(task, num_dependencies)

        elif mode == TSP.COMPLETE:
            # handle task-complete TSP
            assert len(data) == 4
            global_ts, thread, task, parent_task = data

            # 1. If the parent is waiting, notify it of a met dependency
            self.task_pool.notify_task_complete(task, parent_task)

            # 2. If the parent now has 0 unmet dependencies, move it to set of
            # ready tasks

        else:
            raise ValueError(f"unkown task scheduling point: {mode=}, {data=}")

        # After handling TSP:

        # A. For each available thread:
        #   - determine tasks eligible to be scheduled on this thread
        #   - call a handler to decide what to do (start/resume/do nothing)

        # B. If a task was scheduled (either started/resumed) enqueue the
        # generated TSPs

        # C. Remove any scheduled task from the set of ready tasks

        # D. Ensure TSPs remain sorted in temporal order
        self._task_scheduling_points = deque(
            sorted(self._task_scheduling_points, key=lambda tsp: tsp[1])
        )

    def tasks_pending(self):
        return self.task_pool.tasks_pending()

    def task_scheduling_points_pending(self):
        return len(self._task_scheduling_points) > 0

    def set_next_available_ts(self, thread_id: int, time: int):
        self.next_available_ts[thread_id] = time


class Model:
    """Creates the scheduler with the given number of threads"""

    def __init__(self, task_pool: TaskPool, num_threads: int = 1) -> None:
        self.scheduler = TaskScheduler(task_pool, num_threads)

    def run(self, max_steps: Optional[int] = None):
        initial_tasks = self.scheduler.task_pool.get_ready_tasks()
        print(f"task pool contains {len(initial_tasks)} initial tasks: {initial_tasks}")
        steps = 0
        while self.scheduler.task_scheduling_points_pending():
            print("[STEP]")
            self.scheduler.step()
            steps += 1
            if max_steps is not None and steps > max_steps:
                break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("anchorfile", help="the Otter OTF2 anchorfile to use")
    otter.args.add_common_arguments(parser)
    args = parser.parse_args()
    otter.log.initialise(args)
    project = otter.project.BuildGraphFromDB(args.anchorfile)
    print(f"simulating trace {args.anchorfile}")
    with ExitStack() as ctx:
        con = ctx.enter_context(project.connection())
        reader = ctx.enter_context(otf2_ext.open_trace(args.anchorfile))
        seek_events = ctx.enter_context(reader.seek_events())
        chunk_reader = DBChunkReader(reader.attributes, seek_events, con)
        model = Model(TaskPool(con, chunk_reader), num_threads=4)
        model.run(max_steps=3)
