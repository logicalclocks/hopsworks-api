-- brazil_volume_by_year: Brazilian suppliers' share of the American market for the part type, per year (TPC-H Q8).
select
    o_year,
    make_date(o_year, 1, 1) as year_start,
    sum(brazil_volume) / sum(volume) as mkt_share,
    sum(brazil_volume) as brazil_volume,
    sum(volume) as america_volume
from {{ ref("processed") }}
group by o_year
order by o_year
