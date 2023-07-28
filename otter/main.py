"""Access Otter via the command line"""

import argparse
import sys
from enum import Enum

import otter


class Action(str, Enum):
    """Defines the available actions"""

    UNPACK = "unpack"
    SHOW = "show"


class GraphType(str, Enum):
    """Defines the graph types available to do_show()"""

    CFG = "cfg"  # control-flow graph
    HIER = "hier"  # task hierarchy


description_action = {
    Action.UNPACK: "unpack an Otter OTF2 trace and prepare it for querying by other Otter actions",
    Action.SHOW: "visualise a chosen task's graph or the task hierarchy",
}


description_show = {
    GraphType.CFG: "show the control-flow graph of a chosen task",
    GraphType.HIER: "show the task hierarchy",
}


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add common arguments to a parser"""
    debug_choices = ["DEBUG", "INFO", "WARN", "ERROR"]
    parser.add_argument("anchorfile", help="the Otter OTF2 anchorfile to use")
    parser.add_argument(
        "--loglevel",
        dest="loglevel",
        default="WARN",
        choices=debug_choices,
        help=f"logging level (one of {', '.join(debug_choices)})",
        metavar="level",
    )
    parser.add_argument(
        "--logdir",
        dest="logdir",
        default="otter-logs",
        help="logging directory",
        metavar="dir",
    )


def prepare_parser():
    """Prepare the main parser and all subparsers"""

    formatter_class = argparse.ArgumentDefaultsHelpFormatter
    parser = argparse.ArgumentParser(formatter_class=formatter_class)

    # subparsers for each action (unpack, show, ...)
    subparse_action = parser.add_subparsers(dest="action")

    # parse the unpack action
    parse_action_unpack = subparse_action.add_parser(
        Action.UNPACK.value,
        help=description_action[Action.UNPACK],
        description=description_action[Action.UNPACK],
        formatter_class=formatter_class,
    )
    add_common_arguments(parse_action_unpack)

    # parse the show action
    parse_action_show = subparse_action.add_parser(
        Action.SHOW.value,
        help=description_action[Action.SHOW],
        description=description_action[Action.SHOW],
        formatter_class=formatter_class,
    )

    # subparsers for each graph type to show (cfg, hier, ...)
    subparse_action_show = parse_action_show.add_subparsers(dest="show")

    # parse the action "show cfg"
    parser_show_cfg = subparse_action_show.add_parser(
        GraphType.CFG.value,
        help=description_show[GraphType.CFG],
        description=description_show[GraphType.CFG],
        formatter_class=formatter_class,
    )
    parser_show_cfg.add_argument("task", help="task ID", type=int)
    parser_show_cfg.add_argument(
        "-o",
        "--out",
        dest="dotfile",
        metavar="dotfile",
        help="where to save the graph",
        default="cfg_{task}.dot",
    )
    parser_show_cfg.add_argument(
        "-s",
        "--style",
        dest="style",
        help="apply styling to the graph",
        action="store_true",
        default=False,
    )
    parser_show_cfg.add_argument(
        "--simple",
        dest="simple",
        help="create a simplified graph",
        action="store_true",
        default=False,
    )
    add_common_arguments(parser_show_cfg)

    # parse the action "show hier"
    parser_show_hier = subparse_action_show.add_parser(
        GraphType.HIER.value,
        help=description_show[GraphType.HIER],
        description=description_show[GraphType.HIER],
        formatter_class=formatter_class,
    )
    parser_show_hier.add_argument(
        "-o",
        "--out",
        dest="dotfile",
        metavar="dotfile",
        help="where to save the graph",
        default="hier.dot",
    )
    add_common_arguments(parser_show_hier)

    return parser


def select_action() -> None:
    """Select an action and forward the arguments to that action"""

    parser = prepare_parser()
    args = parser.parse_args()

    otter.log.initialise(args)

    log = otter.log.get_logger("main")
    if log.isEnabledFor(otter.log.DEBUG):
        log.info("arguments:")
        for arg, value in vars(args).items():
            log.info("  %s=%s", arg, value)

    debug = args.loglevel == "DEBUG"

    if args.action == Action.UNPACK:
        do_unpack(args.anchorfile, debug=debug)
    elif args.action == Action.SHOW:
        if args.show == GraphType.CFG:
            do_show_cfg(
                args.anchorfile,
                args.dotfile,
                args.task,
                args.style,
                args.simple,
                debug=debug,
            )
        elif args.show == GraphType.HIER:
            do_show_hierarchy(args.anchorfile, args.dotfile, debug=debug)
    else:
        print(f"unknown action: {args.action}")
        parser.print_help()


def do_unpack(anchorfile: str, debug: bool = False) -> None:
    """Unpack a trace into a db for querying"""

    try:
        project = otter.project.ReadTasksProject(anchorfile, debug=debug)
    except NotADirectoryError as err:
        otter.log.error("directory not found: %s", err)
        raise SystemExit(1) from err
    project.create_db_from_trace()
    project.quit()


def do_show_cfg(
    anchorfile: str,
    dotfile: str,
    task: int,
    style: bool = False,
    simple: bool = False,
    debug: bool = False,
) -> None:
    """Show the cfg of a given task"""

    if "{task}" in dotfile:
        dotfile = dotfile.format(task=task)

    otter.log.info(" --> STEP: create project")
    project = otter.project.BuildGraphFromDB(anchorfile, debug=debug)
    with project.connection() as con:
        otter.log.info(" --> STEP: build cfg (task=%d)", task)
        if simple:
            cfg = project.build_control_flow_graph_simplified(
                con,
                task,
                keys=["flavour", "task_label", "init_file", "init_func", "init_line"],
                debug=debug,
            )
        else:
            cfg = project.build_control_flow_graph(con, task, debug=debug)
        if debug:
            otter.log.debug("cfg vertex attributes:")
            for name in cfg.vs.attributes():
                otter.log.debug(" -- %s", name)
        if style:
            otter.log.info(" --> STEP: style cfg (task=%d)", task)
            cfg = project.style_graph(con, cfg, debug=debug)
        else:
            otter.log.info(" --> [ * SKIPPED * ] STEP: style cfg (task=%d)", task)
    otter.log.info(" --> STEP: write cfg to file (dotfile=%s)", dotfile)
    project.write_graph_to_file(cfg, filename=dotfile)
    otter.log.info(" --> STEP: convert to svg")
    result, _, stderr, svgfile = project.convert_dot_to_svg(dotfile)
    if result != 0:
        for line in stderr:
            print(line, file=sys.stderr)
    else:
        project.log.info("cfg for task %d written to %s", task, svgfile)


def do_show_hierarchy(anchorfile: str, dotfile: str, debug: bool = False) -> None:
    """Show the task hierarchy of a trace"""

    project = otter.project.ReadTasksProject(anchorfile, debug=debug, prepare_env=False)
    hierarchy = project.build_parent_child_graph()
    project.write_graph_to_file(hierarchy, filename=dotfile)
    project.log.info("task hierarchy graph written to %s", dotfile)
