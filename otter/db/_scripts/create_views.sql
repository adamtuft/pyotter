-- Create views of the tables in a tasks db

-- Get a readable view of the tasks' creation locations
create view if not exists task_create_location(
         id
        ,file
        ,func
        ,line
    ) as
    select task.id as id
        ,file.text as file
        ,func.text as func
        ,src.line as line
    from task
    inner join task_history as hist
        on hist.id = task.id
        and hist.action = 1 -- create
    inner join source as src
        on src.src_loc_id = hist.source_location_id
    inner join string as file
        on file.id = src.file_id
    inner join string as func
        on func.id = src.func_id
;

-- Get a readable view of the tasks' start locations
create view if not exists task_start_location(
         id
        ,file
        ,func
        ,line
    ) as
    select task.id as id
        ,file.text as file
        ,func.text as func
        ,src.line as line
    from task
    inner join task_history as hist
        on hist.id = task.id
        and hist.action = 2 -- start
    inner join source as src
        on src.src_loc_id = hist.source_location_id
    inner join string as file
        on file.id = src.file_id
    inner join string as func
        on func.id = src.func_id
;

-- Get a readable view of the tasks' end locations
create view if not exists task_end_location(
         id
        ,file
        ,func
        ,line
    ) as
    select task.id as id
        ,file.text as file
        ,func.text as func
        ,src.line as line
    from task
    inner join task_history as hist
        on hist.id = task.id
        and hist.action = 3 -- end
    inner join source as src
        on src.src_loc_id = hist.source_location_id
    inner join string as file
        on file.id = src.file_id
    inner join string as func
        on func.id = src.func_id
;

-- Union of all source locations
create view if not exists task_location as
    select *,
           'create' as type
    from task_create_location
    union
    select *,
           'start' as type
    from task_start_location
    union
    select *,
           'end' as type
    from task_end_location
    order by id
        ,type
;

-- A readable view of a task's attributes (label, etc)
create view if not exists task_attributes as
    select task.id
        ,parent.parent_id
        ,count(children.child_id) as num_children
        ,string.text as task_label
        ,crt.time as create_ts
        ,start.time as start_ts
        ,end.time as end_ts
        ,create_loc.file as create_file
        ,create_loc.func as create_func
        ,create_loc.line as create_line
        ,start_loc.file as start_file
        ,start_loc.func as start_func
        ,start_loc.line as start_line
        ,end_loc.file as end_file
        ,end_loc.func as end_func
        ,end_loc.line as end_line
    from task
    left join task_history as crt
        on task.id = crt.id
        and crt.action = 1 -- create
    left join task_history as start
        on task.id = start.id
        and start.action = 2 -- start
    left join task_history as end
        on task.id = end.id
        and end.action = 3 -- end
    left join task_relation as parent
        on task.id = parent.child_id
    left join string
        on task.user_label = string.id
    left join task_create_location as create_loc
        on task.id = create_loc.id
    left join task_start_location as start_loc
        on task.id = start_loc.id
    left join task_end_location as end_loc
        on task.id = end_loc.id
    left join task_relation as children
        on task.id = children.parent_id
	group by task.id
;

-- 
create view if not exists _critical_tasks as
    with descendant as (
        select 0 as id
        union all
        select crit.critical_child as id
        from descendant
        inner join critical_task as crit
        on descendant.id = crit.id
    )
    select attr.*
    from descendant
    inner join task_attributes as attr
        on descendant.id = attr.id
;

--------------------------------------------------------------------------------
--
-- Views related to the simulated schedule
--
--------------------------------------------------------------------------------

