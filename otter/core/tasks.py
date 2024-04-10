from __future__ import annotations

from dataclasses import dataclass

from otter.db.types import SourceLocation


@dataclass
class TaskData:
    id: int
    parent_id: int
    task_label: str
    crt_ts: int
    create_location: SourceLocation
