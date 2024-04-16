-- Get the location refs and positions of a task's events
select location_ref
    ,location_count
from task_history
where id = ?
order by time
;
