-- orders.select_data: the order, its customer and date.
select o_orderkey, o_custkey, o_orderdate
from {{ source("tpch", "orders") }}
