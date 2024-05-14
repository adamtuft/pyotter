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
    id int not null,                     -- task ID
    action int not null,                 -- 
    time not null,                       -- time of action
    source_location_id int not null,     -- source location
    location_ref int not null,           -- location ref of the evt writer
    location_count int not null,         -- position in the evt writer's stream
    cpu int not null,                    -- cpu of encountering thread
    tid int not null,                    -- thread ID
    foreign key (id) references task (id)
);

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

-- 
create table critical_task(
    sim_id int not null,   -- partition the separate simulations
    id int not null,
    sequence int not null,
    critical_child int not null,
    primary key (sim_id, id, sequence)
    foreign key (id) references task (id)
);


--------------------------------------------------------------------------------
--
-- Tables related to the simulated schedule
--
--------------------------------------------------------------------------------

-- List actions of each task, using partial keys to enforce uniqueness of some actions
create table sim_task_history(
    sim_id int not null,   -- partition the separate simulations
    id int not null,       -- task ID
    action int not null,   -- 
    time not null,         -- time of action
    source_location_id,    -- source location
    cpu int not null,      -- cpu of encountering thread
    tid int not null,      -- thread ID
    foreign key (id) references task (id)
);

-- List metadata about each task-suspend action in a simulated schedule
create table sim_task_suspend_meta(
    sim_id int not null,   -- partition the separate simulations
    id int not null,       -- task ID
    time not null,         -- time of action
    sync_descendants int not null,
    primary key (sim_id, id, time)
    foreign key (id) references task (id)
);
