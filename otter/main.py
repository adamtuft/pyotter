"""Access Otter via the command line"""

from json import dumps

import otter
from otter.args import Action, GraphType


def select_action() -> None:
    """Select an action and forward the arguments to that action"""

    args = otter.args.parse()

    if args.version:
        print(otter.__version__)
        raise SystemExit(0)

    if args.print_args:
        print(dumps(vars(args), indent=2))

    if args.action is None:
        otter.args.print_help()
        raise SystemExit(0)

    otter.log.initialise(args)

    debug = args.loglevel == otter.log.Level.DEBUG

    with otter.profile.output(args.profile):
        if args.action == Action.UNPACK:
            otter.project.unpack_trace(args.anchorfile, debug=debug)
        elif args.action == Action.SHOW:
            if args.show == GraphType.CFG:
                otter.project.show_control_flow_graph(
                    args.anchorfile,
                    args.dotfile,
                    args.task,
                    args.style,
                    args.simple,
                    debug=debug,
                )
            elif args.show == GraphType.HIER:
                otter.project.show_task_hierarchy(
                    args.anchorfile, args.dotfile, debug=debug
                )
        elif args.action == Action.SUMMARY:
            otter.project.summarise_tasks_db(args.anchorfile, debug=debug)
        else:
            print(f"unknown action: {args.action}")
            otter.args.print_help()

    raise SystemExit(0)
