from __future__ import annotations

from typing import Optional, Set, Tuple, Callable

import otter.log

from otter.db.protocols import (
    TaskMetaCallback,
    TaskActionCallback,
    TaskSuspendMetaCallback,
)

from otter.db.types import SourceLocation

from otter.core.events import Event, Location
from otter.core.tasks import TaskData
from otter.definitions import (
    Attr,
    EventModel,
    EventType,
    NullTaskID,
)

from .event_model import (
    BaseEventModel,
    EventModelFactory,
    TraceEventIterable,
)


@EventModelFactory.register(EventModel.TASKGRAPH)
class TaskGraphEventModel(BaseEventModel):

    def __init__(
        self,
        *args,
        gather_return_addresses: Optional[Set[int]] = None,
        **kwargs,
    ):
        super().__init__()
        self._return_addresses = gather_return_addresses

    def event_completes_chunk(self, event: Event) -> bool:
        return event.event_type == EventType.task_leave

    def event_updates_chunk(self, event: Event) -> bool:
        return event.event_type == EventType.task_enter

    def event_skips_chunk_update(self, event: Event) -> bool:
        """The task-create event for the root task shouldn't be added to any chunk as it doesn't have a parent"""
        return event.event_type == EventType.task_create and event.unique_id == 0

    def is_task_register_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_create

    def is_task_create_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_create

    def is_update_task_start_ts_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_enter

    def is_update_duration_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch

    def is_chunk_start_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_enter

    def get_tasks_switched(self, event: Event) -> Tuple[int, int]:
        return event.parent_task_id, event.unique_id

    def is_task_complete_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_leave

    def is_task_sync_event(self, event: Event) -> bool:
        """Use task-sync-begin as the sync event as this easily captures task-create events in the parent"""
        return event.event_type == EventType.sync_begin

    def is_task_suspend_event(self, event: Event) -> bool:
        """Matches both taskwait enter & taskwait discrete"""
        return event.event_type == EventType.sync_begin

    def is_task_resume_event(self, event: Event) -> bool:
        return event.event_type == EventType.sync_end

    def get_task_completed(self, event: Event) -> int:
        return event.encountering_task_id

    @staticmethod
    def get_task_entered(event: Event) -> int:
        return event.encountering_task_id

    def get_task_registered_data(self, event: Event) -> TaskData:
        assert self.is_task_register_event(event)
        return TaskData(
            event.unique_id,
            event.encountering_task_id,
            event.task_label,
            event.time,
            SourceLocation(event.source_file, event.source_func, event.source_line),
        )

    def get_source_location(self, event: Event) -> SourceLocation:
        return SourceLocation(event.source_file, event.source_func, event.source_line)

    def _pre_yield_event_callback(self, event: Event) -> None:
        """Called once for each event before it is sent to super().yield_chunks"""
        return

    def _post_yield_event_callback(self, event: Event) -> None:
        """Called once for each event after it has been sent to super().yield_chunks"""
        if self._return_addresses is not None and hasattr(event, Attr.caller_return_address.value):
            address = event.caller_return_address
            if address not in self._return_addresses:
                self._return_addresses.add(address)

    def _filter_event(self, event: Event) -> bool:
        """Return True if an event should be processed when yielding chunks"""
        if event.is_buffer_flush_event():
            otter.log.warning("buffer flush event encountered - skipped (%s)", event)
            return False
        return True

    def _filter_with_callbacks(self, events_iter: TraceEventIterable) -> TraceEventIterable:
        for location, location_count, event in events_iter:
            if self._filter_event(event):
                self._pre_yield_event_callback(event)
                yield location, location_count, event
                self._post_yield_event_callback(event)

    def apply_callbacks(
        self,
        events_iter: TraceEventIterable,
        add_task_metadata_cbk: TaskMetaCallback,
        add_task_action_cbk: TaskActionCallback,
        add_task_suspend_meta_cbk: TaskSuspendMetaCallback,
        interval: int,
        interval_callback: Callable[[int], None],
    ):
        return super().apply_callbacks(
            self._filter_with_callbacks(events_iter),
            add_task_metadata_cbk,
            add_task_action_cbk,
            add_task_suspend_meta_cbk,
            interval,
            interval_callback,
        )
