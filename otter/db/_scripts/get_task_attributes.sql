-- Get a task's attributes. Don't join strings for source locations here,
-- memoize the source locations themselves in Python

select task.id
    ,task.parent_id
    ,task.num_children
    ,task.create_ts
    ,task.start_ts
    ,task.end_ts
    ,label.text
    ,task.create_location
    ,task.start_location
    ,task.end_location
from task
left join string as label
    on task.user_label = label.id
where task.id in ({placeholder})
order by task.id
;
