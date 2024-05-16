-- List the events of a task
with events as (
	select *
		,(row_number() over (order by id, time))-1 as event
	from task_history
	where task_history.id in (?)
)
select events.id
    ,events.event
	,events.action
	,src.file_name
	,src.func_name
	,src.line
	,events.time
from events
left join source_location as src
    on events.source_location_id = src.src_loc_id
order by events.id
	,events.time
;
