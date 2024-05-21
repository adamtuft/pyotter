from typing import Optional, Callable, Mapping, Tuple, Any
from functools import partial
from collections import Counter
from itertools import count

import pandas as pd
import otter
from matplotlib import pyplot as plt

from otter.db.types import Task, TaskSchedulingState
from otter.db import ReadConnection
from otter.definitions import TaskAction
from otter.reporting import colour_picker

ColourMap = Mapping[Any, Tuple[float, float, float]]

COLOUR_WHITE = (1, 1, 1)
COLOUR_BLACK = (0, 0, 0)
COLOUR_GREY = (0.88,) * 3
COLOUR_RED = (1, 0, 0)
COLOUR_YELLOW = (1, 1, 0)
COLOUR_BLUE = (0, 0, 1)
COLOUR_MAGENTA = (1, 0, 1)
COLOUR_DARK_GREEN = (0.29, 0.404, 0.255)
COLOUR_ORANGE = (1, 0.643, 0.125)
X_SCALE_FACTOR = 1_000_000_000

ALPHA_FULL = 1.00
ALPHA_DEFAULT = 0.85
ALPHA_LIGHT = 0.25
ALPHA_NOSHOW = 0.00


def is_root_task(task: Task):
    return task.id == 0


def is_phase_task(task: Task):
    return task.attr.label.startswith("OTTER PHASE")


def is_leaf_task(task: Task):
    return task.children == 0


def is_task_create_state(state: TaskSchedulingState):
    return state.action_start == TaskAction.CREATE


def get_phase_colour(state: TaskSchedulingState, reader: ReadConnection, get_colour: ColourMap):
    task = reader.get_task(state.task)
    if is_phase_task(task):
        if state.is_active:
            return ((1, 1, 1), COLOUR_RED, ALPHA_FULL)
        return ((1, 1, 1), COLOUR_RED, ALPHA_FULL)
    return ((0, 0, 0), (0, 0, 0), ALPHA_NOSHOW)


def get_phase_plotting_data(state: TaskSchedulingState, **kwargs):
    face, edge, alpha = get_phase_colour(state, **kwargs)
    base = {
        "xrange": (state.start_ts / X_SCALE_FACTOR, state.duration / X_SCALE_FACTOR),
        "facecolor": face,
        "edgecolor": edge,
        "alpha": alpha,
    }
    return base


def make_yaxis_key(state: TaskSchedulingState, reader: ReadConnection, **kwargs) -> Any:
    return (state.tid_start, reader.get_task(state.task).attr.create_location)


def get_state_colour(
    state: TaskSchedulingState,
    pred: Callable[[Task], bool],
    reader: ReadConnection,
    get_colour: ColourMap,
):
    task = reader.get_task(state.task)
    if is_root_task(task) or is_phase_task(task):
        return (get_colour[task.attr.label], COLOUR_WHITE, ALPHA_NOSHOW)
    else:
        if pred(task):
            if state.is_active:
                return (COLOUR_YELLOW, COLOUR_RED, ALPHA_FULL)
            return (COLOUR_RED, COLOUR_RED, ALPHA_FULL)
        elif state.is_active:
            return (get_colour[task.attr.label], COLOUR_GREY, ALPHA_LIGHT)
        elif state.action_start == TaskAction.CREATE:
            return (COLOUR_BLUE, COLOUR_GREY, ALPHA_NOSHOW)
        elif state.action_start == TaskAction.SUSPEND:
            return (COLOUR_RED, COLOUR_GREY, ALPHA_LIGHT)
        else:
            otter.log.debug(f"no colour for {state=}")
            return (COLOUR_MAGENTA, COLOUR_GREY, ALPHA_LIGHT)


def get_state_plotting_data(state, **kwargs):
    face, edge, alpha = get_state_colour(state, **kwargs)
    base = {
        "xrange": (state.start_ts / X_SCALE_FACTOR, state.duration / X_SCALE_FACTOR),
        "ykey": make_yaxis_key(state, **kwargs),
        "facecolor": face,
        "edgecolor": edge,
        "alpha": alpha,
    }
    return base


def get_task_crt_plotting_data(state, **kwargs):
    base = {
        "x": state.start_ts / X_SCALE_FACTOR,
        "ykey": make_yaxis_key(state, **kwargs),
    }
    return base


def plot_scheduling_data(anchorfile: str, /, *, title: Optional[str] = None):
    otter.log.info(f"plotting from anchorfile: {anchorfile}")

    title = title or f"Scheduling Data (trace={anchorfile})"
    reader = otter.project.ReadTraceData(anchorfile).connect()
    get_colour = colour_picker()
    all_tasks = list(reader.iter_all_task_ids())

    #! A temporary hack here
    otter.log.debug("get critical tasks")
    critical_tasks = list(
        item[0]
        for item in reader._con.execute(
            "select distinct critical_child from critical_task where sim_id = 0 order by critical_child"
        )
    )

    def is_critical_task(task: Task):
        return task.id in critical_tasks

    otter.log.debug("get non-phase tasks")
    non_phase_tasks = [
        task.id
        for task in reader.iter_all_tasks()
        if not (is_root_task(task) or is_phase_task(task))
    ]

    otter.log.debug("get non-phase tasks scheduling states")
    scheduling_states = reader.get_task_scheduling_states(non_phase_tasks)

    otter.log.debug("build non-phase tasks scheduling states dataframe")
    state_df = pd.DataFrame(
        map(
            partial(
                get_state_plotting_data, reader=reader, pred=is_critical_task, get_colour=get_colour
            ),
            scheduling_states,
        )
    )

    otter.log.debug("build task-create scheduling states dataframe")
    task_crt_df = pd.DataFrame(
        map(
            partial(
                get_task_crt_plotting_data,
                reader=reader,
                pred=is_critical_task,
                get_colour=get_colour,
            ),
            filter(is_task_create_state, scheduling_states),
        )
    )

    otter.log.debug("prepare plot")
    fig, ax = plt.subplots()
    ykeys = list(set(state_df["ykey"]))
    ykeys.sort()
    num_rows = len(ykeys)

    phase_tasks = [task.id for task in reader.iter_all_tasks() if is_phase_task(task)]
    phase_sched = reader.get_task_scheduling_states(phase_tasks)
    phase_sched_df = pd.DataFrame(
        map(partial(get_phase_plotting_data, reader=reader, get_colour=get_colour), phase_sched)
    )

    # print the phase scheduling data
    for s in phase_sched:
        if s.action_start == TaskAction.CREATE:
            continue
        if s.action_start == TaskAction.START:
            task = reader.get_task(s.task)
            print(f"  PHASE ID: {s.task:>8d} [label: {task.attr.label}]")
            print(
                f"  {'TIME':<15s} | {' DURATION':<15s} | {' CHILDREN':<9s} | {' DESC.':<9s} | {' ACTION':<9s} | {' LOCATION':<14s}"
            )
            print("=" * 80)
        children = reader.get_children_created_between(s.task, s.start_ts, s.end_ts)
        num_children = len(children)
        num_descendants = 0
        for child, _ in children:
            desc = reader.get_descendants_of(child)
            num_descendants += len(desc)
        print(
            f"{s.start_ts:>17,d} | {s.duration:>15,d} | {num_children:>9,d} | {num_descendants:>9,d} | {s.action_start.name:<9s} | {s.start_location}"
        )
        if s.action_end == TaskAction.END:
            print(
                f"{s.end_ts:>17,d} | {'-':>15s} | {'-':>9s} | {'-':>9s} | {s.action_end.name:<9s} | {s.end_location}"
            )
            print()

    otter.log.debug("plot data")

    # plot the phase data
    ax.broken_barh(
        xranges=phase_sched_df["xrange"],
        yrange=(0, num_rows),
        facecolors=phase_sched_df["facecolor"],
        edgecolor=(0.65,) * 3,
        alpha=phase_sched_df["alpha"],
    )

    # plot the regular task data
    otter.log.debug("plotting task scheduling data")
    for ytick, ykey in enumerate(ykeys):
        otter.log.debug(f" -- {ytick=}, {ykey=}")
        rows = state_df[state_df["ykey"] == ykey]
        ax.broken_barh(
            xranges=rows["xrange"],
            yrange=(ytick + 0.075, 0.85),
            facecolors=rows["facecolor"],
            edgecolor=(0, 0, 0),
            alpha=rows["alpha"],
        )
        crt = task_crt_df[task_crt_df["ykey"] == ykey]
        ax.plot(crt["x"], [ytick + 0.5] * len(crt["x"]), "k.")

    ax.set_yticks([x + 0.5 for x in range(len(ykeys))])
    ylabel_depth = set(map(len, ykeys))
    if len(ylabel_depth) != 1:
        otter.log.warning(f"multiple y-label group sizes found: {ylabel_depth}")
        ylabels = [str(k[-1]) for k in ykeys]
    else:
        ylabel_grp_sz = ylabel_depth.pop()
        assert not ylabel_depth
        otter.log.debug(f"y-label group size: {ylabel_grp_sz}")
        ylabel_groups = list(zip(*ykeys))
        ylabel_groups.reverse()
        for offset, ygroup in enumerate(ylabel_groups[1:], start=1):
            df = pd.DataFrame(zip(ygroup, count()))
            span = Counter(ygroup)
            levels = df.groupby(by=0).mean()
            ygroup_labels = ["\n" * offset + str(x) for x in levels.index]
            yrgoup_pos = list(levels.iloc[:, 0])

            # Axis for group labels
            y2 = ax.secondary_yaxis(location=-0.1)
            y2.set_yticks(yrgoup_pos)
            y2.set_yticklabels(ygroup_labels)
            y2.tick_params("y", length=0)
            y2.spines["left"].set_linewidth(0)

            # Axis for lines between groups
            posts = [0]
            for group, v in span.items():
                posts.append(posts[-1] + v)
            y3 = ax.secondary_yaxis(location=0)
            y3.set_yticks(posts, labels=[])
            y3.tick_params("y", length=75, width=1)
            otter.log.debug(f"{ygroup_labels=}, {yrgoup_pos=}, {span=}. {posts=}")

        otter.log.debug(str(ylabel_groups))

    ylabels = [str(k[-1]) for k in ykeys]
    ax.set_yticklabels(ylabels)
    plt.gca().invert_yaxis()
    otter.log.debug(f"set {title=}")
    plt.title(title)
    plt.xlabel("Time")
    plt.show()
    return
