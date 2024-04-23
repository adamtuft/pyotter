-- Create indexes on tables

create unique index idx_task_relation_1
on task_relation(parent_id, child_id)
;

create index idx_task_history_1
on task_history(id, action)
;

-- Partial indexes to enforce uniqueness of create/start/end actions
create unique index idx_task_history_crt
on task_history(id)
where action = 1 -- create
;

create unique index idx_task_history_start
on task_history(id)
where action = 2 -- start
;

create unique index idx_task_history_end
on task_history(id)
where action = 3 -- end
;

create index idx_sim_task_history_1
on sim_task_history(sim_id, id, action)
;

-- Partial indexes to enforce uniqueness of create/start/end actions
create unique index idx_sim_task_history_crt
on sim_task_history(sim_id, id)
where action = 1 -- create
;

create unique index idx_sim_task_history_start
on sim_task_history(sim_id, id)
where action = 2 -- start
;

create unique index idx_sim_task_history_end
on sim_task_history(sim_id, id)
where action = 3 -- end
;
