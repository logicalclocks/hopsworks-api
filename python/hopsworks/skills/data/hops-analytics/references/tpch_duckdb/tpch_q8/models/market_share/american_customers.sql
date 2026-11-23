-- american_customers: customers in an American nation.
select c.c_custkey
from {{ ref("stg_customer") }} as c
join {{ ref("american_nations") }} as an on c.c_nationkey = an.n_nationkey
