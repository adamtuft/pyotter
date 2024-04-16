from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Tuple

from otter.db.types import SourceLocation
from otter.core.events import Event
from otter.core.tasks import TaskData
from otter.definitions import (
    EventModel,
    EventType,
    RegionType,
    TaskStatus,
)

from .event_model import BaseEventModel, EventModelFactory


@EventModelFactory.register(EventModel.OMP)
class OMPEventModel(BaseEventModel):
    def __init__(self, *args, **kwargs):
        super().__init__()
        # A dictionary mapping a single-exec or master-begin event to a list of tasks to be synchronised
        self._task_sync_cache: Dict[Event, List[TaskData]] = defaultdict(list)

    @classmethod
    def event_completes_chunk(cls, event: Event) -> bool:
        return cls.event_updates_and_completes_chunk(event) or (
            cls.is_task_switch_event(event)
            and event.prior_task_status == TaskStatus.complete
        )

    @classmethod
    def event_updates_chunk(cls, event: Event) -> bool:
        return cls.event_updates_and_doesnt_complete_chunk(event) or (
            cls.is_task_switch_event(event)
            and event.prior_task_status != TaskStatus.complete
        )

    @staticmethod
    def event_updates_and_completes_chunk(event: Event) -> bool:
        # parallel-end, single-executor-end, master-end and initial-task-leave always complete a chunk
        return (event.get("region_type"), event.event_type) in [
            (RegionType.parallel, EventType.parallel_end),
            (RegionType.single_executor, EventType.workshare_end),
            (RegionType.master, EventType.master_end),
            (RegionType.initial_task, EventType.task_leave),
        ]

    @staticmethod
    def event_updates_and_doesnt_complete_chunk(event: Event) -> bool:
        # parallel-begin, single-executor-begin, master-begin, initial-task-enter, implicit-task-enter/leave
        # these events have special chunk-updating logic but never complete a chunk
        return (event.get("region_type"), event.event_type) in [
            (RegionType.parallel, EventType.parallel_begin),
            (RegionType.single_executor, EventType.workshare_begin),
            (RegionType.master, EventType.master_begin),
            (RegionType.initial_task, EventType.task_enter),
            (RegionType.implicit_task, EventType.task_enter),
            (RegionType.implicit_task, EventType.task_leave),
        ]

    @staticmethod
    def event_skips_chunk_update(event: Event) -> bool:
        # thread-begin/end events aren't represented in chunks, so won't update them
        return event.event_type in [EventType.thread_begin, EventType.thread_end]

    @classmethod
    def is_event_type(cls, event: Event, event_type: EventType) -> bool:
        return event.event_type == event_type

    @classmethod
    def is_task_register_event(cls, event: Event) -> bool:
        # True for: TaskEnter, TaskCreate
        return event.event_type in (EventType.task_enter, EventType.task_create)

    @classmethod
    def is_task_create_event(cls, event: Event) -> bool:
        return cls.is_event_type(event, EventType.task_create)

    @classmethod
    def is_task_enter_event(cls, event: Event) -> bool:
        return cls.is_event_type(event, EventType.task_enter)

    @classmethod
    def is_task_leave_event(cls, event: Event) -> bool:
        return cls.is_event_type(event, EventType.task_leave)

    @classmethod
    def is_task_switch_event(cls, event: Event) -> bool:
        return cls.is_event_type(event, EventType.task_switch)

    @classmethod
    def is_update_task_start_ts_event(cls, event: Event) -> bool:
        return cls.is_task_enter_event(event) or (
            cls.is_task_switch_event(event) and not cls.is_task_complete_event(event)
        )

    @classmethod
    def get_task_entered(cls, event: Event) -> int:
        if cls.is_task_enter_event(event):
            return event.unique_id
        elif cls.is_task_switch_event(event):
            return event.next_task_id
        else:
            raise NotImplementedError(f"{event}")

    @classmethod
    def is_update_duration_event(cls, event: Event) -> bool:
        return event.event_type in (
            EventType.task_switch,
            EventType.task_enter,
            EventType.task_leave,
        )

    @classmethod
    def get_tasks_switched(cls, event: Event) -> Tuple[int, int]:
        if event.event_type in (EventType.task_enter, EventType.task_leave):
            return event.encountering_task_id, event.unique_id
        elif cls.is_task_switch_event(event):
            return event.encountering_task_id, event.next_task_id
        else:
            raise NotImplementedError(f"{event}")

    @classmethod
    def is_task_complete_event(cls, event: Event) -> bool:
        return cls.is_task_leave_event(event) or (
            cls.is_task_switch_event(event)
            and event.prior_task_status in (TaskStatus.complete, TaskStatus.cancel)
        )

    @classmethod
    def is_task_switch_complete_event(cls, event: Event) -> bool:
        return event.prior_task_status in [TaskStatus.complete, TaskStatus.cancel]

    @classmethod
    def is_task_group_end_event(cls, event: Event) -> bool:
        return (event.region_type, event.event_type) == (
            RegionType.taskgroup,
            EventType.sync_end,
        )

    @classmethod
    def get_task_completed(cls, event: Event) -> int:
        if cls.is_task_leave_event(event):
            return event.unique_id
        elif cls.is_task_switch_event(event):
            assert cls.is_task_complete_event(event)
            return event.encountering_task_id

    @classmethod
    def get_task_data(cls, event: Event) -> TaskData:
        # Only defined for RegisterTaskDataMixin classes
        # i.e. task-enter, task-create
        assert cls.is_task_register_event(event)
        return TaskData(
            event.unique_id,
            event.parent_task_id,
            event.task_flavour,
            event.task_label,
            event.time,
            SourceLocation(
                event.task_init_file, event.task_init_func, event.task_init_line
            ),
        )
