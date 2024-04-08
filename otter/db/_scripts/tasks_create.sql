-- Set up a database of tasks

-- List the tasks
create table task(
    id int unique not null,

    -- these fields removed as they can be calculated from the task_history table
    -- start_ts,
    -- end_ts,
    -- duration int,

    -- these fields removed as they are now in the task_history table
    -- init_loc_id int not null,  -- the location where the task was initialised
    -- start_loc_id int not null, -- the location where the task started
    -- end_loc_id int not null,   -- the location where the task ended

    flavour int,
    user_label int,
    primary key (id)

    -- these keys removed as their fields removed
    -- foreign key (init_loc_id) references source (src_loc_id),
    -- foreign key (start_loc_id) references source (src_loc_id),
    -- foreign key (end_loc_id) references source (src_loc_id)
);

-- List actions of each task, using partial keys to enforce uniqueness of some actions
create table task_history(
    id int not null,       -- task ID
    action int not null,   -- 
    time not null,         -- time of action
    location_id,           -- source location
    foreign key (id) references task (id)
);

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

-- List metadata about each task-suspend action
create table task_suspend_meta(
    id int not null,       -- task ID
    time not null,         -- time of action
    sync_descendants int not null,
    primary key (id, time)
    foreign key (id) references task (id)
);

-- List parent-child links
create table task_relation(
    parent_id int not null,
    child_id int not null,
    foreign key (parent_id) references task (id),
    foreign key (child_id) references task (id)
);

-- List distinct source location definitions
create table source(
    src_loc_id int not null,
    file_id int not null,
    func_id int not null,
    line int not null,
    primary key (src_loc_id),
    foreign key (file_id) references string (id),
    foreign key (func_id) references string (id)
);

-- List source string definitions
create table string(
    id int not null,
    text,
    primary key (id)
);

-- List the location_ref and local event positions contained in each chunk
create table chunk_contents(
    chunk_key int not null,
    location_ref int not null,
    event_pos int not null,
    primary key (chunk_key, location_ref, event_pos)
);

-- 
create table critical_task(
    id int not null,
    sequence int not null,
    critical_child int not null,
    primary key (id, sequence)
    foreign key (id) references task (id)
);


--------------------------------------------------------------------------------
--
-- Tables related to the simulated schedule
--
--------------------------------------------------------------------------------

-- List unique actions of each task e.g. create, start, end
create table _sim_task_history_unique(
    id int not null,       -- task ID
    action int not null,   -- 
    time not null,         -- time of action
    location_id,           -- source location
    primary key (id, action)
    foreign key (id) references task (id)
);

-- List non-unique actions of each task
create table _sim_task_history_multi(
    id int not null,       -- task ID
    action int not null,   -- 
    time not null,         -- time of action
    location_id,           -- source location
    primary key (id, action, time)
    foreign key (id) references task (id)
);
