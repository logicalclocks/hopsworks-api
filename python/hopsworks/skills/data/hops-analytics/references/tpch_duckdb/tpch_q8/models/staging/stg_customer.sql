-- customer.select_data: the customer and their nation.
select c_custkey, c_nationkey
from {{ source("tpch", "customer") }}
