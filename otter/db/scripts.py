from importlib import resources

from . import _scripts


def get_sql(name: str) -> str:
    return resources.read_text(_scripts, name)


create_tasks: str = get_sql("tasks_create.sql")
create_views: str = get_sql("tasks_create_views.sql")
insert_tasks: str = get_sql("tasks_insert.sql")
insert_task_relations: str = get_sql("tasks_insert_relations.sql")
define_source_locations: str = get_sql("tasks_define_source_locations.sql")
define_strings: str = get_sql("tasks_define_strings.sql")
count_tasks: str = get_sql("tasks_count.sql")
count_tasks_by_attributes: str = get_sql("tasks_count_tasks_by_attributes.sql")
count_children_by_parent_attributes = get_sql("tasks_count_children_by_attributes.sql")
count_chunks = get_sql("chunks_get_num_chunks.sql")
insert_chunk_events: str = get_sql("chunks_insert_event_pos.sql")
get_ancestors: str = get_sql("tasks_get_ancestors.sql")
get_descendants: str = get_sql("tasks_get_descendants.sql")
get_chunk_events: str = get_sql("chunks_get_event_pos.sql")
get_chunk_ids: str = get_sql("chunks_get_chunk_refs.sql")
get_task_scheduling_states: str = get_sql("tasks_get_scheduling_states.sql")
get_children_created_between: str = get_sql("tasks_get_children_created_between.sql")
get_source_location: str = get_sql("get_source_info.sql")
get_string: str = get_sql("get_string.sql")
get_task_attributes: str = get_sql("get_task_attributes.sql")
update_task_locations_times: str = get_sql("tasks_update_locations_times.sql")
update_task_num_children: str = get_sql("tasks_update_num_children.sql")
update_source_info: str = get_sql("source_update_strings.sql")
