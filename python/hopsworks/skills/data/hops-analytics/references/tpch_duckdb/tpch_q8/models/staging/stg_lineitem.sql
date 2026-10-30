-- lineitem.select_data: what each line sold, by whom, and its price after discount.
select l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount
from {{ source("tpch", "lineitem") }}
