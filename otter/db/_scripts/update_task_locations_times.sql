-- Update task create/start/end locations and times

update task
set create_location = task_history.location_id
    ,create_ts = task_history.time
from task_history
where task.id = task_history.id
    and task_history.action = 1
;

update task
set start_location = task_history.location_id
    ,start_ts = task_history.time
from task_history
where task.id = task_history.id
    and task_history.action = 2
;

update task
set end_location = task_history.location_id
    ,end_ts = task_history.time
from task_history
where task.id = task_history.id
    and task_history.action = 3
;
