-- List the scheduling states of some tasks
with events as (
	select *
		,row_number() over (order by id, time) as row_number
	from task_history as hist
	where hist.id in ({placeholder})
)
select events_left.id
	,events_left.action as action_start
	,events_right.action as action_end
	,src_left.file_name as file_name_start
	,src_left.func_name as func_name_start
	,src_left.line as line_start
	,src_right.file_name as file_name_end
	,src_right.func_name as func_name_end
	,src_right.line as line_end
	,events_left.time as start_ts
	,events_right.time as end_ts
	,cast(events_right.time as int) - cast(events_left.time as int) as duration
from events as events_left
inner join events as events_right
	on events_left.id = events_right.id
	and events_left.row_number = events_right.row_number-1
left join source_location as src_left
    on events_left.location_id = src_left.src_loc_id
left join source_location as src_right
    on events_right.location_id = src_right.src_loc_id
order by events_left.id
	,start_ts
;
