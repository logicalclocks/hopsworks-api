-- american_orders: the orders of American customers.
select o.o_orderkey, o.o_orderdate
from {{ ref("stg_orders") }} as o
join {{ ref("american_customers") }} as ac on o.o_custkey = ac.c_custkey
