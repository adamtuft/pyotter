from importlib.resources import read_text

from . import _scripts


def script(name):
    return read_text(_scripts, name)


count_children_by_parent_attributes = script("count_children_by_parent_attributes.sql")
count_chunks = script("count_chunks.sql")
count_tasks = script("count_tasks.sql")
count_tasks_by_attributes = script("count_tasks_by_attributes.sql")
create_tasks = script("create_tasks.sql")
create_views = script("create_views.sql")
define_source_locations = script("define_source_locations.sql")
define_strings = script("define_strings.sql")
get_all_task_attributes = script("get_all_task_attributes.sql")
get_ancestors = script("get_ancestors.sql")
get_children_created_between = script("get_children_created_between.sql")
get_chunk_events = script("get_chunk_events.sql")
get_chunk_ids = script("get_chunk_ids.sql")
get_descendants = script("get_descendants.sql")
get_source_location = script("get_source_location.sql")
get_string = script("get_string.sql")
get_task_attributes = script("get_task_attributes.sql")
get_task_scheduling_states = script("get_task_scheduling_states.sql")
insert_chunk_events = script("insert_chunk_events.sql")
insert_task_relations = script("insert_task_relations.sql")
insert_tasks = script("insert_tasks.sql")
update_source_info = script("update_source_info.sql")
update_task_locations_times = script("update_task_locations_times.sql")
update_task_num_children = script("update_task_num_children.sql")
