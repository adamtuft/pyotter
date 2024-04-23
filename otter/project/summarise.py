import otter.log

from otter.args import Summarise

from .project import ReadTraceData


def summarise_tasks_db(
    anchorfile: str,
    summarise: Summarise,
    debug: bool = False,
) -> None:
    """Print summary information about a tasks database"""

    project = ReadTraceData(anchorfile)

    with project.connect() as con:

        if summarise == Summarise.ROWCOUNT:
            for items in con.count_rows():
                print(*items, sep="\t")

        elif summarise == Summarise.SOURCE:
            for location_id, location in con.get_all_source_locations():
                print(location_id, location.file, location.line, location.func, sep="\t")

        elif summarise == Summarise.STRINGS:
            for ref, string in con.get_all_strings():
                print(ref, string, sep="\t")

        elif summarise == Summarise.TASKS:
            for attr, num_tasks in con.iter_all_task_types():
                print(
                    num_tasks,
                    attr.label,
                    f"{attr.create_location.file}:{attr.create_location.line}",
                    f"{attr.start_location.file}:{attr.start_location.line}",
                    f"{attr.end_location.file}:{attr.end_location.line}",
                    sep="\t",
                )

        elif summarise == Summarise.SIMS:
            for sim_id, rows in con.count_simulation_rows():
                print(sim_id, rows, sep="\t")

        else:
            otter.log.error("don't know how to summarise %s", summarise)
