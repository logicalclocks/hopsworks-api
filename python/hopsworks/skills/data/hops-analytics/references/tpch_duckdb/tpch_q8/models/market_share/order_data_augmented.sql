-- order_data_augmented: each line of an American order with its part's type and its supplier's nation.
select
    o.o_orderdate,
    l.l_extendedprice,
    l.l_discount,
    p.p_type,
    n.n_name as supplier_nation
from {{ ref("american_orders") }} as o
join {{ ref("stg_lineitem") }} as l on l.l_orderkey = o.o_orderkey
join {{ ref("stg_supplier") }} as s on l.l_suppkey = s.s_suppkey
join {{ ref("stg_part") }} as p on l.l_partkey = p.p_partkey
join {{ ref("stg_nation") }} as n on s.s_nationkey = n.n_nationkey
