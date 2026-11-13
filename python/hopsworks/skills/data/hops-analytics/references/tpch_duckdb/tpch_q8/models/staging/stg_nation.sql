-- nation.select_data: the nation, its name and region.
select n_nationkey, n_name, n_regionkey
from {{ source("tpch", "nation") }}
