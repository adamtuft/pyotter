from __future__ import annotations

import sys

from collections import defaultdict
from typing import List, Any

import igraph as ig

import otter.log
import otter.db
import otter.reporting

from otter.db.types import TaskAttributes
from otter.definitions import TaskAction
from otter.utils import CountingDict

from .project import ReadTraceData


def make_barrier_vertex(graph, /, *, attr):
    return graph.add_vertex(
        shape="octagon",
        style="filled",
        color="red",
        type="barrier",
        attr=attr,
    )


def build_control_flow_graph(
    con: otter.db.ReadConnection, task: int, debug: bool, simple: bool = False
) -> ig.Graph:
    """Build a task's control-flow-graph"""

    def debug_msg(msg, *args) -> None:
        otter.log.debug("[build_control_flow_graph] " + msg, *args)

    graph = ig.Graph(directed=True)

    (parent,) = con.get_tasks((task,))
    task_descriptor = parent.attr

    # create head & tail vertices
    head = graph.add_vertex(
        shape="plain",
        style="filled",
        attr={
            "id": parent.id,
            "label": task_descriptor.label,
            "created": task_descriptor.create_location,
            "start": task_descriptor.start_location,
            "children": parent.children,
        },
    )
    tail = graph.add_vertex(
        shape="plain",
        style="filled",
        attr={
            "id": parent.id,
            "label": task_descriptor.label,
            "end": task_descriptor.end_location,
        },
    )
    cur = head

    task_states = con.get_task_scheduling_states((task,))
    task_suspend_meta = dict(con.get_task_suspend_meta(task))

    for state in task_states:
        actions = state.action_start, state.action_end

        if state.action_start in (TaskAction.CREATE, TaskAction.SUSPEND):
            continue

        debug_msg(f"{actions=}")

        tasks_created = con.get_children_created_between(task, state.start_ts, state.end_ts)
        if simple:
            child_vertices = [
                graph.add_vertex(
                    attr={
                        "tasks created": len(tasks_created),
                        "start": f"{state.start_location}",
                        "end": f"{state.end_location}",
                    },
                    shape="plain",
                    style="filled",
                    color="yellow",
                )
            ]
        else:
            children = con.get_tasks([child for child, _ in tasks_created])
            child_vertices = [
                graph.add_vertex(
                    shape="plain",
                    style="filled",
                    attr={
                        "id": child.id,
                        "label": child.attr.label,
                        "created": child.attr.create_location,
                        "children": child.children,
                        "duration": int(child.end_ts) - int(child.start_ts),
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

            barrier_vertex = make_barrier_vertex(
                graph,
                attr={
                    "sync mode": task_suspend_meta[state.end_ts].name,
                    "started": f"{state.end_location}",
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

            cur["attr"]["ended"] = f"{state.start_location}"

            if child_vertices:
                for v in child_vertices:
                    graph.add_edge(cur, v)
                    graph.add_edge(v, tail)
            else:
                graph.add_edge(cur, tail)

        elif actions == (TaskAction.RESUME, TaskAction.SUSPEND):
            debug_msg("task resumed after barrier")

            cur["attr"]["ended"] = f"{state.start_location}"

            barrier_vertex = make_barrier_vertex(
                graph,
                attr={
                    "sync mode": task_suspend_meta[state.end_ts].name,
                    "started": f"{state.end_location}",
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
            otter.log.error(f"unhandled: {actions=}")

    if debug:
        debug_msg("created %d vertices:", len(graph.vs))
        for vertex in graph.vs:
            debug_msg("%s", vertex)

    return graph


def style_graph(
    con: otter.db.ReadConnection,
    graph: ig.Graph,
    label_data: List[Any],
    debug: bool = False,
) -> ig.Graph:
    # TODO: could be significantly simpler, all this does is loop over vertices and assign labels (and sometiems colours).
    """Apply styling to the vertices of a graph, using the label data in
    `label_data`.

    If label_data entry:

    - None: leave the label blank
    - int: interpret as a task ID and use the attributes of that task as the label data.
    - dict: use as the label data.
    - tuple: expect a tuple of key-value pairs to be used as the label data
    - other: raise ValueError
    """

    colour = otter.reporting.colour_picker()

    if debug:
        otter.log.debug("label data:")
        for val in label_data:
            if isinstance(val, int):
                otter.log.debug("use task id %d as label key", val)
            elif isinstance(val, dict):
                otter.log.debug("use dict as label data: %s", val)
            elif isinstance(val, tuple):
                otter.log.debug("interpret as key-value pairs: %s", val)

    # For vertices where the key is int, assume it indicates a task and get
    # all such tasks' attributes
    task_id_labels = [x for x in label_data if isinstance(x, int)]
    task_attributes = {attr.id: attr for attr in con.get_tasks(task_id_labels)}
    for label_item, vertex in zip(label_data, graph.vs):
        if label_item is None:
            vertex["label"] = ""
        elif isinstance(label_item, int):
            # Interpret as a task ID and use the task's attributes as the label data
            attributes = task_attributes[label_item]
            # Have id as the first item in the dict
            data: dict[str, Any] = {"id": attributes.id}
            data.update(attributes.attr.asdict())
            vertex["label"] = otter.reporting.as_html_table(data)
            r, g, b = (int(x * 256) for x in colour[data["label"]])
            vertex["color"] = f"#{r:02x}{g:02x}{b:02x}"
        elif isinstance(label_item, dict):
            vertex["label"] = otter.reporting.as_html_table(label_item)
            if vertex["color"] is None:
                r, g, b = (int(x * 256) for x in colour[label_item.get("label")])
                vertex["color"] = f"#{r:02x}{g:02x}{b:02x}"
        elif isinstance(label_item, tuple):
            vertex["label"] = otter.reporting.as_html_table(dict(label_item))
        else:
            raise ValueError(
                f"expected int, dict, None or tuple of name-value pairs, got {type(label_item)}"
            )
    return graph


def show_task_hierarchy(anchorfile: str, dotfile: str, debug: bool = False) -> None:
    """Show the task hierarchy of a trace"""

    project = ReadTraceData(anchorfile)
    otter.log.debug("project=%s", project)

    graph = ig.Graph(directed=True)
    vertices: dict[TaskAttributes, ig.Vertex] = defaultdict(
        lambda: graph.add_vertex(shape="plain", style="filled")
    )

    with project.connect() as con:
        otter.log.debug("fetching data")
        rows = con.get_all_parent_child_attributes()

    if len(rows) == 0:
        otter.log.error("no task hierarchy data was returned")
        raise SystemExit(1)

    for parent, child, total in rows:
        if not child.is_null():
            graph.add_edge(vertices[parent], vertices[child], label=str(total))

    colour = otter.reporting.colour_picker(cycle=True)
    for task, vertex in vertices.items():
        vertex["label"] = otter.reporting.as_html_table(
            task.asdict(),
            rename_keys={
                "create_location": "created",
                "start_location": "start",
                "end_location": "end",
            },
        )
        r, g, b = (int(x * 256) for x in colour[task.label])
        vertex["color"] = f"#{r:02x}{g:02x}{b:02x}"

    otter.log.debug("writing dotfile: %s", dotfile)
    otter.reporting.write_graph_to_file(graph, filename=dotfile)

    otter.log.debug("converting dotfile to svg")
    result, _, stderr, svgfile = otter.reporting.convert_dot_to_svg(dotfile=dotfile, rankdir="LR")
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

    otter.log.info(" --> STEP: create project")
    project = ReadTraceData(anchorfile)
    with project.connect() as con:
        otter.log.info(" --> STEP: build cfg (task=%d)", task)
        cfg = build_control_flow_graph(con, task, debug=debug, simple=simple)
        if style:
            otter.log.info(" --> STEP: style cfg (task=%d)", task)
            cfg = style_graph(con, cfg, cfg.vs["attr"], debug=debug)
        else:
            otter.log.info(" --> [ * SKIPPED * ] STEP: style cfg (task=%d)", task)
    otter.log.info(" --> STEP: write cfg to file (dotfile=%s)", dotfile)
    otter.reporting.write_graph_to_file(cfg, filename=dotfile, drop=["attr", "foo"])
    otter.log.info(" --> STEP: convert to svg")
    result, _, stderr, svgfile = otter.reporting.convert_dot_to_svg(dotfile=dotfile, rankdir="TB")
    if result != 0:
        for line in stderr:
            print(line, file=sys.stderr)
    else:
        otter.log.info("cfg for task %d written to %s", task, svgfile)


def show_task_tree(anchorfile: str, dotfile: str, *, debug: bool = False, rankdir: str) -> None:

    project = ReadTraceData(anchorfile)
    otter.log.debug("project=%s", project)

    graph = ig.Graph(directed=True)
    vertices: dict[int, ig.Vertex] = defaultdict(
        lambda: graph.add_vertex(shape="plain", style="filled")
    )

    label_counter = CountingDict(start=1)
    colour = otter.reporting.colour_picker(cycle=True)

    with project.connect() as con:
        for task in con.iter_all_tasks():
            # count the number of times we see each label
            label_counter.increment(task.attr.label)
            vertex = vertices[task.id]
            label_data = {
                "id": task.id,
                "label": task.attr.label,
                "created": task.attr.create_location,
            }
            vertex["label"] = otter.reporting.as_html_table(label_data)
            vertex["_task_label"] = task.attr.label
            if task.parent is not None:
                graph.add_edge(vertices[task.parent], vertex)

    # for each label seen more than once, colour according to the label
    for vertex in graph.vs:
        task_label = vertex["_task_label"]
        if label_counter[task_label] > 1:
            r, g, b = (int(x * 256) for x in colour[task_label])
            vertex["color"] = f"#{r:02x}{g:02x}{b:02x}"

    del vertex["_task_label"]

    num_vertices = len(graph.vs)
    num_tasks = con.count_tasks()
    if num_vertices != num_tasks:
        otter.log.error(
            "number of tasks (%d) and vertices (%d) don't match!",
            num_tasks,
            num_vertices,
        )
    else:
        otter.log.debug(
            "number of tasks (%d) and vertices (%d) match",
            num_tasks,
            num_vertices,
        )

    otter.log.debug("writing dotfile: %s", dotfile)
    otter.reporting.write_graph_to_file(graph, filename=dotfile)

    otter.log.debug("converting dotfile to svg")
    result, _, stderr, svgfile = otter.reporting.convert_dot_to_svg(
        dotfile=dotfile, rankdir=rankdir
    )
    if result != 0:
        for line in stderr.splitlines():
            print(line, file=sys.stderr)
    else:
        print(f"task tree written to {svgfile}")
