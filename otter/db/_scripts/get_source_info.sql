-- Get source location info

select file_name.text as file
    ,func_name.text as func
    ,line
from source
left join string as file_name
    on source.file_id = file_name.id
left join string as func_name
    on source.func_id = func_name.id
where source.src_loc_id in (?)
;
