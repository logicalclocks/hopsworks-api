-- american_nations: the nations of the AMERICA region.
select n.n_nationkey
from {{ ref("stg_nation") }} as n
join {{ ref("america") }} as a on n.n_regionkey = a.r_regionkey
