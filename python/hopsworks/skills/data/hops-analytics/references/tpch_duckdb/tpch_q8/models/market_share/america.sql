-- america: the AMERICA region.
select r_regionkey
from {{ ref("stg_region") }}
where r_name = 'AMERICA'
