with descendant as (
    select {root_task} as id
    union all
    select crit.critical_child as id
    from descendant
    inner join critical_task as crit
    on descendant.id = crit.id
    where crit.sim_id = ?
)
select id
from descendant
;
