-- processed: each line's volume (price after discount), its order's year, and the volume Brazilian suppliers sold.
select
    supplier_nation,
    extract(year from o_orderdate)::integer as o_year,
    l_extendedprice * (1 - l_discount) as volume,
    case when supplier_nation = 'BRAZIL' then l_extendedprice * (1 - l_discount) else 0 end as brazil_volume
from {{ ref("order_data_filtered") }}
