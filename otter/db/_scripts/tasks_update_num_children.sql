-- Update number of children

update task
set num_children = rel.children
from (
    select task.id
        ,count(rel.child_id) as children
    from task
	left join task_relation as rel
		on task.id = rel.parent_id
    group by task.id
) as rel
where task.id = rel.id
;
