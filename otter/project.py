from __future__ import annotations

import os
import sys
from collections import defaultdict
from contextlib import ExitStack, closing
from typing import Any, AnyStr, Dict, List, Set

import igraph as ig
import otf2_ext

import otter.log as log
import otter.db
import otter.simulator

from . import db, reporting
from .core import Chunk, DBChunkBuilder
from .core.event_model.event_model import (
    EventModel,
    TraceEventIterable,
    get_event_model,
)
from .core.events import Event, Location
from .definitions import SourceLocation, TaskDescriptor, TraceAttr, TaskAction
from .utils import CountingDict, LabellingDict


class Project:
    """Prepare to use an anchorfile as input"""

    def __init__(self, anchorfile: str, debug: bool = False) -> None:

        self.debug = debug
        if self.debug:
            log.debug("using project: %s", self)

        self.anchorfile = os.path.abspath(anchorfile)
        if not os.path.isfile(self.anchorfile):
            log.error("no such file: %s", self.anchorfile)
            raise SystemExit(1)

        self.project_root: str = os.path.dirname(self.anchorfile)
        self.aux_dir = "aux"
        self.maps_file = self.abspath(os.path.join(self.aux_dir, "maps"))

        if not os.path.isdir(self.abspath(self.aux_dir)):
            log.error("directory not found: %s", self.abspath(self.aux_dir))
            raise SystemExit(1)
        if not os.path.isfile(self.maps_file):
            log.error("no such file: %s", self.maps_file)
            raise SystemExit(1)

        self.debug_dir = "debug_output"
        self.source_location_db = self.abspath(os.path.join(self.aux_dir, "srcloc.db"))
        self.tasks_db = self.abspath(os.path.join(self.aux_dir, "tasks.db"))
        self.return_addresses: Set[int] = set()
        self.event_model = None
        self.chunks: list[Chunk] = []

        log.info("project root:  %s", self.project_root)
        log.info("anchorfile:    %s", self.anchorfile)
        log.info("maps file:     %s", self.maps_file)
        log.info("tasks:         %s", self.tasks_db)

    def abspath(self, relname: str) -> AnyStr:
        """Get the absolute path of an internal folder"""
        return os.path.abspath(os.path.join(self.project_root, relname))

    def connection(self) -> closing[db.Connection]:
        """Return a connection to this project's tasks db for use in a with-block"""

        return closing(db.Connection(self.tasks_db))


class UnpackTraceProject(Project):
    """Unpack a trace"""

    def __init__(self, anchorfile: str, debug: bool = False) -> None:
        super().__init__(anchorfile, debug)
        self.source_location_id: Dict[SourceLocation, int] = LabellingDict()
        self.string_id: Dict[str, int] = LabellingDict()

    def prepare_environment(self) -> None:
        """Prepare the environment - create any folders, databases, etc required by the project"""

        debug_dir = self.abspath(self.debug_dir)
        log.info("preparing environment")
        if self.debug:
            if not os.path.exists(debug_dir):
                os.mkdir(debug_dir)
        if os.path.exists(self.tasks_db):
            log.warning("overwriting tasks database %s", self.tasks_db)
            os.remove(self.tasks_db)
        else:
            log.info("creating tasks database %s", self.tasks_db)
        with self.connection() as con:
            log.info(" -- create tables")
            con.executescript(db.scripts.create_tasks)
            log.info(" -- create views")
            con.executescript(db.scripts.create_views)
        log.info("created database %s", self.tasks_db)

    def process_trace(self, con: otter.db.Connection):
        """Read a trace and create a database of tasks"""

        log.info("processing trace")

        chunk_builder = DBChunkBuilder(con, bufsize=5000)
        task_meta_writer = otter.db.DBTaskMetaWriter(con, self.string_id)
        task_action_writer = otter.db.DBTaskActionWriter(con, self.source_location_id)

        # Build the chunks & tasks data
        with ExitStack() as stack:
            reader = stack.enter_context(otf2_ext.open_trace(self.anchorfile))

            log.info("recorded trace version: %s", reader.trace_version)

            if reader.trace_version != otf2_ext.version:
                log.warning(
                    "version mismatch: trace version is %s, python version is %s",
                    reader.trace_version,
                    otf2_ext.version,
                )

            event_model_name = EventModel(
                reader.get_property(TraceAttr.event_model.value)
            )

            self.event_model = get_event_model(
                event_model_name,
                gather_return_addresses=self.return_addresses,
            )

            log.info("found event model name: %s", event_model_name)
            log.info("using event model: %s", self.event_model)

            locations: Dict[int, Location] = {
                ref: Location(location) for ref, location in reader.locations.items()
            }

            # Count the number of events each location yields
            location_counter = CountingDict(start=1)

            # Get the global event reader which streams all events
            global_event_reader = stack.enter_context(reader.events())

            event_iter: TraceEventIterable = (
                (
                    locations[location],
                    location_counter.increment(location),
                    Event(event, reader.attributes),
                )
                for location, event in global_event_reader
            )

            log.info("building chunks")
            log.info("using chunk builder: %s", str(chunk_builder))
            with ExitStack() as temp:
                chunk_builder = temp.enter_context(closing(chunk_builder))
                task_meta_writer = temp.enter_context(closing(task_meta_writer))
                task_action_writer = temp.enter_context(closing(task_action_writer))
                num_chunks = self.event_model.generate_chunks(
                    event_iter,
                    chunk_builder,
                    task_meta_writer.add_task_metadata,
                    task_action_writer.add_task_action,
                    task_action_writer.add_task_suspend_meta,
                )

            log.info("generated %d chunks", num_chunks)

        # Finally, write the definitions of the source locations and then the strings
        log.info("writing trace definitions")

        # TODO: consider refactoring into separate buffered writers in otter.db
        source_location_definitions = (
            (
                locid,
                self.string_id[location.file],
                self.string_id[location.func],
                location.line,
            )
            for (location, locid) in self.source_location_id.items()
        )
        con.executemany(db.scripts.define_source_locations, source_location_definitions)

        string_definitions = (
            (string_key, string) for (string, string_key) in self.string_id.items()
        )
        con.executemany(db.scripts.define_strings, string_definitions)

        con.commit()


class BuildGraphFromDB(Project):
    """Read an existing tasks database to create a graph"""

    def __init__(self, anchorfile: str, debug: bool = False) -> None:
        super().__init__(anchorfile, debug=debug)
        if not os.path.isfile(self.tasks_db):
            log.error("no such file: %s", self.tasks_db)
            raise SystemExit(1)

    def build_control_flow_graph(
        self, con: db.Connection, task: int, debug: bool, simple: bool = False
    ) -> ig.Graph:
        """Build a task's control-flow-graph"""

        def debug_msg(msg, *args) -> None:
            log.debug("[build_control_flow_graph] " + msg, *args)

        graph = ig.Graph(directed=True)

        (parent_attr,) = con.task_attributes((task,))
        task_descriptor = parent_attr.descriptor

        # create head & tail vertices
        head = graph.add_vertex(
            shape="plain",
            style="filled",
            attr={
                "id": parent_attr.id,
                "label": task_descriptor.label,
                "created": task_descriptor.init_location,
                "start": task_descriptor.start_location,
                "children": parent_attr.children,
            },
        )
        tail = graph.add_vertex(
            shape="plain",
            style="filled",
            attr={
                "id": parent_attr.id,
                "label": task_descriptor.label,
                "end": task_descriptor.end_location,
            },
        )
        cur = head

        task_states = con.task_scheduling_states((task,))
        task_suspend_meta = dict(con.task_suspend_meta(task))

        for state in task_states:
            actions = state.action_start, state.action_end

            if state.action_start in (TaskAction.CREATE, TaskAction.SUSPEND):
                # log.debug("ignore inactive states")
                continue

            debug_msg(f"{actions=}")

            tasks_created = con.children_created_between(
                task, state.start_ts, state.end_ts
            )
            if simple:
                child_vertices = [
                    graph.add_vertex(
                        attr={
                            "tasks created": len(tasks_created),
                            "start": f"{state.file_name_start}:{state.line_start}",
                            "end": f"{state.file_name_end}:{state.line_end}",
                        },
                        shape="plain",
                        style="filled",
                        color="yellow",
                    )
                ]
            else:
                children = con.task_attributes([child for child, _ in tasks_created])
                child_vertices = [
                    graph.add_vertex(
                        shape="plain",
                        style="filled",
                        attr={
                            "id": child.id,
                            "label": child.descriptor.label,
                            "created": child.descriptor.init_location,
                            "children": child.children,
                        },
                    )
                    for child in children
                ]

            if actions == (TaskAction.START, TaskAction.END):
                debug_msg("task completed in 1 go")

                if child_vertices:
                    for v in child_vertices:
                        graph.add_edge(cur, v)
                        graph.add_edge(v, tail)
                else:
                    graph.add_edge(cur, tail)

            elif actions == (TaskAction.START, TaskAction.SUSPEND):
                debug_msg("task interrupted at barrier")

                barrier_vertex = graph.add_vertex(
                    shape="octagon",
                    style="filled",
                    color="red",
                    type="barrier",
                    attr={
                        "sync descendants": str(task_suspend_meta[state.end_ts]),
                        "started": f"{state.file_name_end}:{state.line_end}",
                        "ended": "?:?",
                    },
                )

                if child_vertices:
                    for v in child_vertices:
                        graph.add_edge(cur, v)
                        graph.add_edge(v, barrier_vertex)
                else:
                    graph.add_edge(cur, barrier_vertex)

                cur = barrier_vertex

            elif actions == (TaskAction.RESUME, TaskAction.END):
                debug_msg("task completed after barrier")

                cur["attr"]["ended"] = f"{state.file_name_start}:{state.line_start}"

                if child_vertices:
                    for v in child_vertices:
                        graph.add_edge(cur, v)
                        graph.add_edge(v, tail)
                else:
                    graph.add_edge(cur, tail)

            elif actions == (TaskAction.RESUME, TaskAction.SUSPEND):
                debug_msg("task resumed after barrier")

                cur["attr"]["ended"] = f"{state.file_name_start}:{state.line_start}"

                barrier_vertex = graph.add_vertex(
                    shape="octagon",
                    style="filled",
                    color="red",
                    type="barrier",
                    attr={
                        "sync descendants": str(task_suspend_meta[state.end_ts]),
                        "started": f"{state.file_name_end}:{state.line_end}",
                        "ended": "?:?",
                    },
                )
                graph.add_edge(cur, barrier_vertex)

                if child_vertices:
                    for v in child_vertices:
                        graph.add_edge(cur, v)
                        graph.add_edge(v, barrier_vertex)
                else:
                    graph.add_edge(cur, barrier_vertex)

                cur = barrier_vertex

            else:
                log.error(f"unhandled: {actions=}")

        if debug:
            debug_msg("created %d vertices:", len(graph.vs))
            for vertex in graph.vs:
                debug_msg("%s", vertex)

        return graph

    @staticmethod
    def style_graph(
        con: db.Connection,
        graph: ig.Graph,
        label_data: List[Any],
        debug: bool = False,
    ) -> ig.Graph:
        # TODO: could be significantly simpler, all this does is loop over vertices and assign labels (and sometiems colours). Doesn't need to be a member function
        """Apply styling to the vertices of a graph, using the label data in
        `label_data`.

        If label_data entry:

        - None: leave the label blank
        - int: interpret as a task ID and use the attributes of that task as the label data.
        - dict: use as the label data.
        - tuple: expect a tuple of key-value pairs to be used as the label data
        - other: raise ValueError
        """

        colour = reporting.colour_picker()

        if debug:
            log.debug("label data:")
            for val in label_data:
                if isinstance(val, int):
                    log.debug("use task id %d as label key", val)
                elif isinstance(val, dict):
                    log.debug("use dict as label data: %s", val)
                elif isinstance(val, tuple):
                    log.debug("interpret as key-value pairs: %s", val)

        # For vertices where the key is int, assume it indicates a task and get
        # all such tasks' attributes
        task_id_labels = [x for x in label_data if isinstance(x, int)]
        task_attributes = {
            attr.id: attr for attr in con.task_attributes(task_id_labels)
        }
        for label_item, vertex in zip(label_data, graph.vs):
            if label_item is None:
                vertex["label"] = ""
            elif isinstance(label_item, int):
                # Interpret as a task ID and use the task's attributes as the label data
                attributes = task_attributes[label_item]
                # Have id as the first item in the dict
                data: dict[str, Any] = {"id": attributes.id}
                data.update(attributes.descriptor.asdict())
                vertex["label"] = reporting.as_html_table(data)
                r, g, b = (int(x * 256) for x in colour[data["label"]])
                vertex["color"] = f"#{r:02x}{g:02x}{b:02x}"
            elif isinstance(label_item, dict):
                vertex["label"] = reporting.as_html_table(label_item)
                if vertex["color"] is None:
                    r, g, b = (int(x * 256) for x in colour[label_item.get("label")])
                    vertex["color"] = f"#{r:02x}{g:02x}{b:02x}"
            elif isinstance(label_item, tuple):
                vertex["label"] = reporting.as_html_table(dict(label_item))
            else:
                raise ValueError(
                    f"expected int, dict, None or tuple of name-value pairs, got {type(label_item)}"
                )
        return graph


class SimulateFromDB(Project):
    """Read an existing tasks database to simulate a schedule"""

    def __init__(self, anchorfile: str, debug: bool = False) -> None:
        super().__init__(anchorfile, debug=debug)
        if not os.path.isfile(self.tasks_db):
            log.error("no such file: %s", self.tasks_db)
            raise SystemExit(1)


def unpack_trace(anchorfile: str, debug: bool = False) -> None:
    """unpack a trace into a database for querying"""

    log.info("using OTF2 python version %s", otf2_ext.version)

    project = UnpackTraceProject(anchorfile, debug=debug)
    project.prepare_environment()
    with project.connection() as con:
        project.process_trace(con)
        con.print_summary()


def show_task_hierarchy(anchorfile: str, dotfile: str, debug: bool = False) -> None:
    """Show the task hierarchy of a trace"""

    project = BuildGraphFromDB(anchorfile, debug=debug)
    log.debug("project=%s", project)

    graph = ig.Graph(directed=True)
    vertices: dict[TaskDescriptor, ig.Vertex] = defaultdict(
        lambda: graph.add_vertex(shape="plain", style="filled")
    )

    with project.connection() as con:
        log.debug("fetching data")
        rows = con.parent_child_attributes()

    if len(rows) == 0:
        log.error("no task hierarchy data was returned")
        raise SystemExit(1)

    for parent, child, total in rows:
        if not child.is_null():
            graph.add_edge(vertices[parent], vertices[child], label=str(total))

    colour = reporting.colour_picker(cycle=True)
    for task, vertex in vertices.items():
        vertex["label"] = reporting.as_html_table(task.asdict())
        r, g, b = (int(x * 256) for x in colour[task.label])
        vertex["color"] = f"#{r:02x}{g:02x}{b:02x}"

    log.debug("writing dotfile: %s", dotfile)
    reporting.write_graph_to_file(graph, filename=dotfile)

    log.debug("converting dotfile to svg")
    result, _, stderr, svgfile = reporting.convert_dot_to_svg(
        dotfile=dotfile, rankdir="LR"
    )
    if result != 0:
        for line in stderr.splitlines():
            print(line, file=sys.stderr)
    else:
        print(f"task hierarchy graph written to {svgfile}")


def show_control_flow_graph(
    anchorfile: str,
    dotfile: str,
    task: int,
    style: bool,
    simple: bool,
    debug: bool,
) -> None:
    """Show the cfg of a given task"""

    if "{task}" in dotfile:
        dotfile = dotfile.format(task=task)

    log.info(" --> STEP: create project")
    project = BuildGraphFromDB(anchorfile, debug=debug)
    with project.connection() as con:
        log.info(" --> STEP: build cfg (task=%d)", task)
        cfg = project.build_control_flow_graph(con, task, debug=debug, simple=simple)
        if style:
            log.info(" --> STEP: style cfg (task=%d)", task)
            cfg = project.style_graph(con, cfg, cfg.vs["attr"], debug=debug)
        else:
            log.info(" --> [ * SKIPPED * ] STEP: style cfg (task=%d)", task)
    log.info(" --> STEP: write cfg to file (dotfile=%s)", dotfile)
    reporting.write_graph_to_file(cfg, filename=dotfile, drop=["attr", "foo"])
    log.info(" --> STEP: convert to svg")
    result, _, stderr, svgfile = reporting.convert_dot_to_svg(dotfile)
    if result != 0:
        for line in stderr:
            print(line, file=sys.stderr)
    else:
        log.info("cfg for task %d written to %s", task, svgfile)


def summarise_tasks_db(
    anchorfile: str, debug: bool = False, source: bool = False, tasks: bool = False
) -> None:
    """Print summary information about a tasks database"""

    project = BuildGraphFromDB(anchorfile, debug=debug)

    with project.connection() as con:

        title = f"=== SUMMARY OF {con.db} ==="
        print("\n" + "=" * len(title))
        print(title)
        print("=" * len(title) + "\n\n")

        print("::: Tables/Views :::\n")
        con.print_summary()
        print()

        if source:
            print("::: Source Locations :::\n")
            source_locations = con.source_locations()
            for _, location in source_locations:
                print(f"{location.file}:{location.line} in {location.func}")
            print()

        if tasks:
            print("::: Task Types :::\n")
            for descriptor, num_tasks in con.task_types():
                print(f"Count: {num_tasks}")
                print("Data:")
                print(f"  label:    {descriptor.label}")
                print(f"  created:  {descriptor.init_location}")
                print(f"  start:    {descriptor.start_location}")
                print(f"  end:      {descriptor.end_location}")
                print()


def print_filter_to_stdout(include: bool, rules: List[List[str]]) -> None:
    """Print a filter file to stdout"""

    header = [
        "# Otter filter file",
        "# =================",
        "# ",
        "# This filter file defines one or more rules for filtering tasks. Each rule ",
        "# uses one or more key-value pairs to match tasks to the rule. A task",
        "# satisfies a rule if it matches all key-value pairs. Tasks are filtered",
        "# if they match at least one rule.",
        "# ",
    ]

    filter_file: List[str] = [*header]
    mode = "include" if include else "exclude"
    filter_file.append("# whether to exclude or include filtered tasks:")
    filter_file.append(f"{'mode':<8s} {mode}")
    filter_file.append("")
    for n, rule in enumerate(rules, start=1):
        keys = set()
        filter_file.append(f"# rule {n}:")
        for item in rule:
            split_at = item.find("=")
            key, value = item[0:split_at], item[split_at + 1 :]
            filter_file.append(f"{key:<8s} {value}")
            if key in keys:
                print(f'Error parsing rule {n}: repeated key "{key}"', file=sys.stderr)
                raise SystemExit(1)
            else:
                keys.add(key)
        filter_file.append(f"# end rule {n}")
        filter_file.append("")

    for line in filter_file:
        print(line)


def simulate_schedule(anchorfile: str, debug: bool = False) -> None:

    project = SimulateFromDB(anchorfile, debug)
    with project.connection() as con:
        log.info(f"simulating trace {anchorfile}")
        otter.simulator.simulate_ideal(con)
