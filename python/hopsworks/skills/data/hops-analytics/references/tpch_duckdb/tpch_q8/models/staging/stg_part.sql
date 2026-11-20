-- part.select_data: the part and its type.
select p_partkey, p_type
from {{ source("tpch", "part") }}
