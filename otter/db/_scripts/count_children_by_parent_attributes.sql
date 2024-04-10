select parent_label.text
    ,parent.create_location
    ,parent.start_location
    ,parent.end_location
    ,child_label.text
    ,child.create_location
    ,child.start_location
    ,child.end_location
    ,count(distinct child.id) as child_tasks
from task as parent
inner join task as child
    on parent.id = child.parent_id
left join string as parent_label
    on parent.user_label = parent_label.id
left join string as child_label
    on child.user_label = child_label.id
group by 1,2,3,4,5,6,7,8
order by parent_label.text
    ,child_tasks desc
;
