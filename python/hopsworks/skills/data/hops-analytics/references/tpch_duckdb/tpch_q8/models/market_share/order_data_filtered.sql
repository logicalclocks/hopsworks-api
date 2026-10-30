-- order_data_filtered: the lines ordered between start_date and end_date, of parts of type p_type.
select *
from {{ ref("order_data_augmented") }}
where o_orderdate between date '{{ var("start_date") }}' and date '{{ var("end_date") }}'
  and p_type = '{{ var("p_type") }}'
