from dataclasses import dataclass, InitVar, field, asdict
from typing import NamedTuple

from otter.definitions import TaskAction


class SourceLocation(NamedTuple):
    file: str = "?"
    func: str = "?"
    line: int = 0

    def __str__(self) -> str:
        return f"{self.file}:{self.line} in {self.func}"


@dataclass(frozen=True)
class TaskSchedulingState:
    task: int
    action_start: int
    action_end: int
    file_name_start: InitVar[str]
    func_name_start: InitVar[str]
    line_start: InitVar[int]
    file_name_end: InitVar[str]
    func_name_end: InitVar[str]
    line_end: InitVar[int]
    start_ts: str
    end_ts: str
    duration: int
    start_location: SourceLocation = field(init=False)
    end_location: SourceLocation = field(init=False)

    def __post_init__(
        self,
        file_name_start: str,
        func_name_start: str,
        line_start: int,
        file_name_end: str,
        func_name_end: str,
        line_end: int,
    ) -> None:
        super().__setattr__(
            "start_location",
            SourceLocation(file_name_start, func_name_start, line_start),
        )
        super().__setattr__(
            "end_location",
            SourceLocation(file_name_end, func_name_end, line_end),
        )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(task={self.task}, {TaskAction(self.action_start)}:{self.start_location} at {self.start_ts} -> {TaskAction(self.action_end)}:{self.start_location} at {self.end_ts})"

    def asdict(self):
        return asdict(self)


@dataclass(frozen=True)
class TaskAttributes:  # For use in Connection.parent_child_attributes
    # Describes the invariant attributes of a task i.e. those compiled into the annotations

    label: str
    init_location: SourceLocation
    start_location: SourceLocation
    end_location: SourceLocation

    def is_null(self) -> bool:
        return self.label is None

    def asdict(self):
        return asdict(self)


@dataclass(frozen=True)
class Task:
    # Represents a row in the task table

    id: int
    parent: int
    children: int
    create_ts: str
    start_ts: str
    end_ts: str
    label: InitVar[str]
    init_location: InitVar[SourceLocation]
    start_location: InitVar[SourceLocation]
    end_location: InitVar[SourceLocation]
    attr: TaskAttributes = field(init=False)

    def __post_init__(
        self,
        label: str,
        init_location: SourceLocation,
        start_location: SourceLocation,
        end_location: SourceLocation,
    ) -> None:
        super().__setattr__(
            "attr",
            TaskAttributes(
                label,
                init_location,
                start_location,
                end_location,
            ),
        )

    def is_null(self) -> bool:
        return self.attr.is_null()

    def asdict(self):
        return asdict(self)
