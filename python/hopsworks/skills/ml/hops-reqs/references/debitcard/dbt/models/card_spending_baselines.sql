with latest as (
    select
        *,
        row_number() over (partition by card_number order by feature_date desc) as recency
    from {{ ref('card_daily_features') }}
)

select
    card_number,
    feature_ts as baseline_time,
    feature_date as baseline_date,
    avg_daily_amount_eur_30d,
    cast(txn_count_7d as double) / 7.0 as avg_daily_txn_count_7d,
    cast(declined_count_30d as double) / nullif(cast(txn_count_30d as double), 0.0) as decline_ratio_30d,
    cast(high_risk_count_30d as double) / nullif(cast(txn_count_30d as double), 0.0) as high_risk_ratio_30d,
    max_merchant_distance_km_30d
from latest
where recency = 1
