from __future__ import annotations

import os
from typing import Set
from contextlib import contextmanager

import otter.log
import otter.db
import otter.simulator

from otter.core import Chunk

from prettytable import PrettyTable


class Project:
    """Prepare to use an anchorfile as input"""

    def __init__(self, anchorfile: str, debug: bool = False) -> None:

        self.debug = debug
        if self.debug:
            otter.log.debug("using project: %s", self)

        self.anchorfile = os.path.abspath(anchorfile)
        if not os.path.isfile(self.anchorfile):
            otter.log.error("no such file: %s", self.anchorfile)
            raise SystemExit(1)

        self.project_root: str = os.path.dirname(self.anchorfile)
        self.aux_dir = "aux"
        self.maps_file = self.abspath(os.path.join(self.aux_dir, "maps"))

        if not os.path.isdir(self.abspath(self.aux_dir)):
            otter.log.error("directory not found: %s", self.abspath(self.aux_dir))
            raise SystemExit(1)
        if not os.path.isfile(self.maps_file):
            otter.log.error("no such file: %s", self.maps_file)
            raise SystemExit(1)

        self.source_location_db = self.abspath(os.path.join(self.aux_dir, "srcloc.db"))
        self.tasks_db = self.abspath(os.path.join(self.aux_dir, "tasks.db"))
        self.return_addresses: Set[int] = set()
        self.event_model = None
        self.chunks: list[Chunk] = []

        otter.log.info("project root:  %s", self.project_root)
        otter.log.info("anchorfile:    %s", self.anchorfile)
        otter.log.info("maps file:     %s", self.maps_file)
        otter.log.info("tasks:         %s", self.tasks_db)

    def abspath(self, relname: str):
        """Get the absolute path of an internal folder"""
        return os.path.abspath(os.path.join(self.project_root, relname))

    def connection(self, /, **kwargs):
        """Return a connection to this project's tasks db"""
        return otter.db.Connection(self.tasks_db, **kwargs)

    @contextmanager
    def prepare_connection(self, summarise: bool = True):
        con = self.connection(overwrite=True, initialise=True)
        otter.log.info("created database %s", self.tasks_db)
        yield con
        con.finalise()
        con.commit()
        if summarise:
            table = PrettyTable(["Table/View", "Name", "Rows"], align="l")
            table.add_rows(con.count_rows())
            print(table)
        con.close()


class ReadTraceData(Project):
    """Read data from an unpacked trace"""

    def __init__(self, anchorfile: str, debug: bool = False) -> None:
        super().__init__(anchorfile, debug=debug)
        if not os.path.isfile(self.tasks_db):
            otter.log.error("no such file: %s", self.tasks_db)
            raise SystemExit(1)
