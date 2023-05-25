from . import _scripts

try:
    import importlib.resources as resources
except ImportError:
    import importlib_resources as resources

def get_sql(name: str) -> str:
    return resources.read_text(_scripts, name)

create_tasks: str = get_sql("tasks_create.sql")
create_views: str = get_sql("tasks_create_views.sql")
insert_tasks: str = get_sql("tasks_insert.sql")
insert_task_relations: str = get_sql("tasks_insert_relations.sql")
define_source_locations: str = get_sql("tasks_define_source_locations.sql")
define_source_strings: str = get_sql("tasks_define_source_strings.sql")
count_tasks: str = get_sql("tasks_count.sql")
