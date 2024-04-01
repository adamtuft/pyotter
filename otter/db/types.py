from dataclasses import dataclass, InitVar, field, asdict
from typing import NamedTuple

from otter.definitions import TaskAction


class SourceLocation(NamedTuple):
    file: str = "?"
    func: str = "?"
    line: int = 0

    def __str__(self) -> str:
        return f"{self.file}:{self.line} in {self.func}"


class TaskSchedulingState(NamedTuple):
    task: int
    action_start: int
    action_end: int
    file_name_start: str
    line_start: int
    file_name_end: str
    line_end: int
    start_ts: str
    end_ts: str
    duration: int

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(task={self.task}, {TaskAction(self.action_start)}:{self.start_ts} -> {TaskAction(self.action_end)}:{self.end_ts})"


@dataclass(frozen=True)
class TaskDescriptor:  # For use in Connection.parent_child_attributes
    # Describes the invariant attributes of a task i.e. those compiled into the annotations

    label: str
    flavour: int
    init_file: InitVar[str]
    init_func: InitVar[str]
    init_line: InitVar[int]
    start_file: InitVar[str]
    start_func: InitVar[str]
    start_line: InitVar[int]
    end_file: InitVar[str]
    end_func: InitVar[str]
    end_line: InitVar[int]
    init_location: SourceLocation = field(init=False)
    start_location: SourceLocation = field(init=False)
    end_location: SourceLocation = field(init=False)

    def __post_init__(
        self,
        init_file: str,
        init_func: str,
        init_line: int,
        start_file: str,
        start_func: str,
        start_line: int,
        end_file: str,
        end_func: str,
        end_line: int,
    ) -> None:
        super().__setattr__(
            "init_location", SourceLocation(init_file, init_func, init_line)
        )
        super().__setattr__(
            "start_location", SourceLocation(start_file, start_func, start_line)
        )
        super().__setattr__(
            "end_location", SourceLocation(end_file, end_func, end_line)
        )

    def is_null(self) -> bool:
        return self.label is None and self.flavour is None

    def asdict(self):
        return asdict(self)


@dataclass(frozen=True)
class TaskAttributes:
    # Describes the variant (runtime-dependent) and invariant attributes of a task
    # Contains the fields returned by the `task_attributes` view

    id: int
    parent: int
    children: int
    flavour: InitVar[int]
    label: InitVar[str]
    create_ts: str
    start_ts: str
    end_ts: str
    init_file: InitVar[str]
    init_func: InitVar[str]
    init_line: InitVar[int]
    start_file: InitVar[str]
    start_func: InitVar[str]
    start_line: InitVar[int]
    end_file: InitVar[str]
    end_func: InitVar[str]
    end_line: InitVar[int]
    descriptor: TaskDescriptor = field(init=False)

    def __post_init__(
        self,
        flavour: int,
        label: str,
        init_file: str,
        init_func: str,
        init_line: int,
        start_file: str,
        start_func: str,
        start_line: int,
        end_file: str,
        end_func: str,
        end_line: int,
    ) -> None:
        super().__setattr__(
            "descriptor",
            TaskDescriptor(
                label,
                flavour,
                init_file,
                init_func,
                init_line,
                start_file,
                start_func,
                start_line,
                end_file,
                end_func,
                end_line,
            ),
        )

    def is_null(self) -> bool:
        return self.descriptor.is_null()

    def asdict(self):
        return asdict(self)
