from __future__ import annotations

from enum import Enum


class Attr(str, Enum):
    time = "time"
    event_type = "event_type"
    region_type = "region_type"
    endpoint = "endpoint"
    unique_id = "unique_id"
    encountering_task_id = "encountering_task_id"
    task_type = "task_type"
    parent_task_id = "parent_task_id"
    next_task_id = "next_task_id"
    edge_type = "edge_type"
    prior_task_id = "prior_task_id"
    prior_task_status = "prior_task_status"
    next_task_region_type = "next_task_region_type"
    source_file_name = "source_file_name"
    source_func_name = "source_func_name"
    source_line_number = "source_line_number"
    phase_type = "phase_type"
    task_label = "task_label"
    source_line = "source_line"
    source_file = "source_file"
    source_func = "source_func"
    task_init_line = "task_init_line"
    task_init_file = "task_init_file"
    task_init_func = "task_init_func"
    caller_return_address = "caller_return_address"
    sync_descendant_tasks = "sync_descendant_tasks"


class EventType(str, Enum):
    thread_begin = "thread_begin"
    thread_end = "thread_end"
    parallel_begin = "parallel_begin"
    parallel_end = "parallel_end"
    workshare_begin = "workshare_begin"
    workshare_end = "workshare_end"
    sync_begin = "sync_begin"
    sync_end = "sync_end"
    task_create = "task_create"
    task_schedule = "task_schedule"
    task_enter = "task_enter"
    task_leave = "task_leave"
    task_switch = "task_switch"
    master_begin = "master_begin"
    master_end = "master_end"
    phase_begin = "phase_begin"
    phase_end = "phase_end"


class RegionType(str, Enum):
    parallel = "parallel"
    workshare = "workshare"
    sync = "sync"
    task = "task"
    initial_task = "initial_task"
    implicit_task = "implicit_task"
    explicit_task = "explicit_task"
    target_task = "target_task"
    sections = "sections"
    single_executor = "single_executor"
    single_other = "single_other"
    distribute = "distribute"
    loop = "loop"
    taskloop = "taskloop"
    master = "master"
    barrier = "barrier"
    barrier_implicit = "barrier_implicit"
    barrier_explicit = "barrier_explicit"
    barrier_implementation = "barrier_implementation"
    taskwait = "taskwait"
    taskgroup = "taskgroup"
    generic_phase = "generic_phase"
    PARALLEL = "parallel"
    WORKSHARE = "workshare"
    SYNC = "sync"
    TASK = "task"


class TaskStatus(str, Enum):
    complete = "complete"
    taskyield = "yield"
    cancel = "cancel"
    detach = "detach"
    early_fulfil = "early_fulfil"
    late_fulfil = "late_fulfil"
    switch = "switch"


class TaskType(str, Enum):
    initial = "initial_task"
    implicit = "implicit_task"
    explicit = "explicit_task"
    target = "target_task"


class TaskAction(int, Enum):
    # INIT = 0
    CREATE = 1
    START = 2
    END = 3
    SUSPEND = 4
    RESUME = 5


class Endpoint(str, Enum):
    enter = "enter"
    leave = "leave"
    discrete = "discrete"


class EdgeType(str, Enum):
    execution_flow = "execution_flow"
    taskwait = "taskwait"
    taskgroup = "taskgroup"


#! deprecate this in favour of TaskSyncMode
class TaskSyncType(int, Enum):
    children = 0
    descendants = 1


class TaskSyncMode(int, Enum):
    CHILDREN = 0
    DESCENDANTS = 1
    YIELD = 2


class TaskEvent(str, Enum):
    CREATE = EventType.task_create.value
    SWITCH = EventType.task_switch.value


class TraceAttr(str, Enum):
    event_model = "OTTER::EVENT_MODEL"


class EventModel(str, Enum):
    OMP = "OMP"
    TASKGRAPH = "TASKGRAPH"
    UNKNOWN = "UNKNOWN"


NullTaskID = 18446744073709551615
