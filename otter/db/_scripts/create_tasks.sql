-- Set up a database of tasks

-- List the tasks
create table task(
    id int unique not null,
    parent_id int,
    num_children int,     -- set during finalisation
    user_label int,
    create_ts,            -- set during finalisation        
    start_ts,             -- set during finalisation       
    end_ts,               -- set during finalisation     
    create_location int,  -- set during finalisation
    start_location int,   -- set during finalisation
    end_location int,     -- set during finalisation
    primary key (id),
    foreign key (user_label) references string (id),
    foreign key (create_location) references source (src_loc_id),
    foreign key (start_location) references source (src_loc_id),
    foreign key (end_location) references source (src_loc_id)
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
