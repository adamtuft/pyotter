from __future__ import annotations

from abc import ABC, abstractmethod

from typing import Dict, Iterable, Tuple, Callable

import otter.log
import otter

from otter.db.types import SourceLocation
from otter.db.protocols import (
    TaskMetaCallback,
    TaskActionCallback,
    TaskSuspendMetaCallback,
)
from otter.core.events import Event, Location
from otter.core.tasks import TaskData
from otter.definitions import EventModel, NullTaskID, TaskAction, TaskSyncType

# Type hint aliases
TraceEventIterable = Iterable[Tuple[Location, int, Event]]


# Using ABC for a common __init__ between concrete models
class BaseEventModel(ABC):

    @abstractmethod
    def event_completes_chunk(self, event: Event) -> bool:
        """Return True if an event marks the end of a chunk. This event causes a completed chunk to be emitted."""
        raise NotImplementedError()

    @abstractmethod
    def event_updates_chunk(self, event: Event) -> bool:
        """Return True if an event has some bespoke chunk-updating logic to apply, but does not mark the end of any chunk."""
        raise NotImplementedError()

    @abstractmethod
    def event_skips_chunk_update(self, event: Event) -> bool:
        """Return True if an event does not update any chunks i.e. it is not represented in a chunk."""
        raise NotImplementedError()

    @abstractmethod
    def is_task_create_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def is_task_register_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def is_chunk_start_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def is_update_task_start_ts_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def is_update_duration_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get_tasks_switched(self, event: Event) -> Tuple[int, int]:
        raise NotImplementedError()

    @abstractmethod
    def is_task_complete_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def is_task_sync_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def is_task_suspend_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def is_task_resume_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get_task_completed(self, event: Event) -> int:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def get_task_entered(event: Event) -> int:
        raise NotImplementedError()

    @abstractmethod
    def get_task_registered_data(self, event: Event) -> TaskData:
        raise NotImplementedError()

    @abstractmethod
    def get_source_location(self, event: Event) -> SourceLocation:
        """Get the source location of this event"""
        raise NotImplementedError()

    def apply_callbacks(
        self,
        events_iter: TraceEventIterable,
        add_task_metadata_cbk: TaskMetaCallback,
        add_task_action_cbk: TaskActionCallback,
        add_task_suspend_meta_cbk: TaskSuspendMetaCallback,
        interval: int,
        interval_callback: Callable[[int], None],
    ):
        otter.log.debug("receiving events from %s", events_iter)

        total_events = 0
        for k, (location, location_count, event) in enumerate(events_iter, start=1):
            otter.log.debug(
                "got event %d (location=%s, position=%d): %s",
                k,
                location,
                location_count,
                event,
            )

            # Update the task builder
            if self.is_task_register_event(event):
                task = self.get_task_registered_data(event)
                parent_id = task.parent_id if task.parent_id != NullTaskID else None
                add_task_metadata_cbk(task.id, parent_id, task.task_label)
                add_task_action_cbk(
                    task.id,
                    TaskAction.CREATE,
                    str(event.time),
                    task.create_location,
                    location_ref=location.ref,
                    location_count=location_count,
                    cpu=event.cpu,
                    tid=event.tid,
                )
            if self.is_update_task_start_ts_event(event):
                add_task_action_cbk(
                    self.get_task_entered(event),
                    TaskAction.START,
                    str(event.time),
                    self.get_source_location(event),
                    location_ref=location.ref,
                    location_count=location_count,
                    cpu=event.cpu,
                    tid=event.tid,
                )
            if self.is_task_complete_event(event):
                add_task_action_cbk(
                    self.get_task_completed(event),
                    TaskAction.END,
                    str(event.time),
                    self.get_source_location(event),
                    location_ref=location.ref,
                    location_count=location_count,
                    cpu=event.cpu,
                    tid=event.tid,
                )
            if self.is_task_suspend_event(event):
                add_task_action_cbk(
                    event.encountering_task_id,
                    TaskAction.SUSPEND,
                    str(event.time),
                    self.get_source_location(event),
                    location_ref=location.ref,
                    location_count=location_count,
                    cpu=event.cpu,
                    tid=event.tid,
                )
                add_task_suspend_meta_cbk(
                    event.encountering_task_id,
                    str(event.time),
                    bool(event.sync_descendant_tasks == TaskSyncType.descendants),
                    event.sync_mode,
                )
            elif self.is_task_resume_event(event):
                add_task_action_cbk(
                    event.encountering_task_id,
                    TaskAction.RESUME,
                    str(event.time),
                    self.get_source_location(event),
                    location_ref=location.ref,
                    location_count=location_count,
                    cpu=event.cpu,
                    tid=event.tid,
                )

            total_events = k

            if (total_events % interval) == 0:
                interval_callback(total_events)

        return total_events


class EventModelFactory:
    event_models: Dict[EventModel, type[BaseEventModel]] = {}

    @classmethod
    def get_model_class(cls, model_name: EventModel) -> type[BaseEventModel]:
        """Get the class representing a particular event model"""
        return cls.event_models[model_name]

    @classmethod
    def register(cls, model_name: EventModel):
        """Create a decorator which registers that a class represents the given event model"""

        def wrapper(model_class: type[BaseEventModel]):
            cls.event_models[model_name] = model_class
            return model_class

        return wrapper


def get_event_model(model_name: EventModel, *args, **kwargs) -> BaseEventModel:
    cls = EventModelFactory.get_model_class(model_name)
    return cls(*args, **kwargs)
