-- supplier.select_data: the supplier and their nation.
select s_suppkey, s_nationkey
from {{ source("tpch", "supplier") }}
