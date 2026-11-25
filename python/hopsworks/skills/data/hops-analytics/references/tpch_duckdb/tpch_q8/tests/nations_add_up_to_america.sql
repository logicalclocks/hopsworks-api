-- The nations' volumes add up to the American volume of each year.
select b.o_year
from {{ ref("brazil_volume_by_year") }} as b
join (
    select o_year, sum(volume) as volume
    from {{ ref("america_volume_by_nation_year") }}
    group by o_year
) as n on n.o_year = b.o_year
where abs(n.volume - b.america_volume) > 0.01
