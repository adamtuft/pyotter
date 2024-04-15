select type
    ,name
from sqlite_master
where type in ('table', 'view')
order by type, name
;
