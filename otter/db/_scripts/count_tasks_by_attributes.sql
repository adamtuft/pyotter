select task_label.text
    ,task.create_location
    ,task.start_location
    ,task.end_location
    ,count(*) as num_tasks
from task
left join string as task_label
    on task.user_label = task_label.id
group by 1,2,3,4
order by num_tasks desc
    ,task_label.text
;
