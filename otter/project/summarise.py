import otter.log

from otter.args import Summarise

from .project import ReadTraceData


def summarise_tasks_db(
    anchorfile: str,
    summarise: Summarise,
    debug: bool = False,
) -> None:
    """Print summary information about a tasks database"""

    project = ReadTraceData(anchorfile, debug=debug)

    with project.connection() as con:

        if summarise == Summarise.ROWCOUNT:
            con.print_row_count(sep="\t")

        elif summarise == Summarise.SOURCE:
            con.print_source_locations(sep="\t")

        elif summarise == Summarise.STRINGS:
            con.print_strings(sep="\t")

        elif summarise == Summarise.TASKS:
            con.print_tasks(sep="\t")

        else:
            otter.log.error("don't know how to summarise %s", summarise)
