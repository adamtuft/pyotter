from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path

from otter.log import Loggable
import otter.db
import otter.simulator


class ProjectBase(ABC, Loggable):
    """Prepare to use an anchorfile as input"""

    def __init__(self, anchorfile: str, /) -> None:
        self.log_debug("using project: %s", self)
        self.anchorfile = Path(anchorfile).resolve()
        self.project_root: str = os.path.dirname(self.anchorfile)
        self.aux_dir = "aux"
        self.maps_file = self.abspath(os.path.join(self.aux_dir, "maps"))

        if not os.path.isdir(self.abspath(self.aux_dir)):
            self.log_error("directory not found: %s", self.abspath(self.aux_dir))
            raise SystemExit(1)
        if not os.path.isfile(self.maps_file):
            self.log_error("no such file: %s", self.maps_file)
            raise SystemExit(1)

        self.log_info("project root:  %s", self.project_root)
        self.log_info("anchorfile:    %s", self.anchorfile)

    def abspath(self, relname: str):
        """Get the absolute path of an internal folder"""
        return os.path.abspath(os.path.join(self.project_root, relname))

    @abstractmethod
    def connect(self, /, **kwargs): ...


class UnpackTraceData(ProjectBase):

    def connect(self, /, overwrite: bool):
        return otter.db.WriteConnection(Path(self.project_root), overwrite=overwrite)


class ReadTraceData(ProjectBase):
    """Read data from an unpacked trace"""

    def connect(self, /):
        """Return a connection to this project's tasks db"""
        return otter.db.ReadConnection(Path(self.project_root))


class SimulateTrace(ProjectBase):
    """Read native trace data and write a simulated schedule"""

    def __init__(self, anchorfile: str) -> None:
        super().__init__(anchorfile)
        self._reader = otter.db.ReadConnection(Path(self.project_root))

    @property
    def reader(self):
        return self._reader

    def connect(self, /):
        return otter.db.WriteSimConnection(Path(self.project_root))
