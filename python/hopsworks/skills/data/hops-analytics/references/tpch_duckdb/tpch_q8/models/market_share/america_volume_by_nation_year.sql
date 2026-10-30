-- america_volume_by_nation_year: the same market split by every supplier nation, which the dashboard charts.
select
    o_year,
    make_date(o_year, 1, 1) as year_start,
    supplier_nation,
    sum(volume) as volume
from {{ ref("processed") }}
group by o_year, supplier_nation
order by o_year, supplier_nation
