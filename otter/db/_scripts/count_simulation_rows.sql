select sim_id
    ,count(*) as rows
from sim_task_history
group by sim_id
;
