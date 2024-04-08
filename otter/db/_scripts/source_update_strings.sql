-- Create a table mapping source location to strings

create table source_location as
select source.src_loc_id
    ,file_name.text as file_name
    ,func_name.text as func_name
    ,line
from source
left join string as file_name
    on source.file_id = file_name.id
left join string as func_name
    on source.func_id = func_name.id
;
