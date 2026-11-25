-- region.select_data: the region's key and name.
select r_regionkey, r_name
from {{ source("tpch", "region") }}
