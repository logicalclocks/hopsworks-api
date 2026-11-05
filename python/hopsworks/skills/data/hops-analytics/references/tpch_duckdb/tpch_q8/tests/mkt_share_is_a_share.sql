-- A market share is between 0 and 1, and Brazil's volume is part of America's.
select *
from {{ ref("brazil_volume_by_year") }}
where mkt_share < 0 or mkt_share > 1 or brazil_volume > america_volume
