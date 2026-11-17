with authorisations as (
    select
        transaction_id,
        card_number,
        transaction_time,
        amount_eur,
        network_risk_score,
        issuer_fraud_score,
        merchant_id,
        authorisation_region,
        card_scheme,
        merchant_distance_km,
        cash_withdrawals_48h_eur,
        cvc2_check_result,
        pos_entry_mode,
        authorisation_decision,
        queued_for_review
    from {{ source('cards', 'authorisations') }}
),

labels as (
    select
        card_number,
        transaction_time,
        max(fraud_flag) as fraud_flag
    from {{ source('cards', 'authorisation_signals') }}
    group by card_number, transaction_time
),

daily as (
    select
        a.card_number,
        date(a.transaction_time) as feature_date,
        max(a.transaction_time) as feature_ts,
        cast(count(*) as bigint) as txn_count,
        sum(a.amount_eur) as total_amount_eur,
        avg(a.amount_eur) as avg_amount_eur,
        max(a.amount_eur) as max_amount_eur,
        coalesce(stddev_pop(a.amount_eur), 0.0) as amount_stddev_eur,
        cast(count_if(a.authorisation_decision = 'D') as bigint) as declined_count,
        cast(count_if(a.queued_for_review = 'Y') as bigint) as review_count,
        cast(count_if(a.network_risk_score > 500 or a.issuer_fraud_score > 500) as bigint) as high_risk_count,
        cast(count_if(a.cvc2_check_result = 'N') as bigint) as cvc2_failure_count,
        cast(count_if(a.pos_entry_mode in ('09', '80', '81')) as bigint) as ecommerce_or_token_count,
        cast(approx_distinct(a.merchant_id) as bigint) as distinct_merchants,
        cast(approx_distinct(a.authorisation_region) as bigint) as distinct_regions,
        cast(approx_distinct(a.card_scheme) as bigint) as distinct_card_schemes,
        avg(a.merchant_distance_km) as avg_merchant_distance_km,
        max(a.merchant_distance_km) as max_merchant_distance_km,
        max(a.cash_withdrawals_48h_eur) as max_cash_withdrawals_48h_eur,
        avg(a.network_risk_score) as avg_network_risk_score,
        avg(cast(a.issuer_fraud_score as double)) as avg_issuer_fraud_score,
        cast(count_if(coalesce(l.fraud_flag, 0) = 1) as bigint) as fraud_count,
        cast(max(coalesce(l.fraud_flag, 0)) as integer) as day_fraud_flag
    from authorisations a
    left join labels l
        on a.card_number = l.card_number
       and a.transaction_time = l.transaction_time
    group by a.card_number, date(a.transaction_time)
),

rolling as (
    select
        *,
        sum(txn_count) over last_7 as txn_count_7d,
        sum(total_amount_eur) over last_7 as total_amount_eur_7d,
        sum(txn_count) over last_30 as txn_count_30d,
        sum(declined_count) over last_30 as declined_count_30d,
        sum(high_risk_count) over last_30 as high_risk_count_30d,
        avg(total_amount_eur) over last_30 as avg_daily_amount_eur_30d,
        max(max_merchant_distance_km) over last_30 as max_merchant_distance_km_30d,
        sum(fraud_count) over last_30 as fraud_count_30d,
        lead(day_fraud_flag) over (partition by card_number order by feature_date) as fraud_next_active_day
    from daily
    -- Windows over the card's active days, the days it was used, not calendar days.
    window
        last_7 as (partition by card_number order by feature_date rows between 6 preceding and current row),
        last_30 as (partition by card_number order by feature_date rows between 29 preceding and current row)
)

select
    concat(card_number, '|', cast(feature_date as varchar)) as card_day_id,
    card_number,
    feature_date,
    feature_ts,
    cast(day_of_week(feature_date) as integer) as day_of_week,
    cast(case when day_of_week(feature_date) in (6, 7) then 1 else 0 end as integer) as is_weekend,
    txn_count,
    total_amount_eur,
    avg_amount_eur,
    max_amount_eur,
    amount_stddev_eur,
    declined_count,
    cast(declined_count as double) / nullif(cast(txn_count as double), 0.0) as decline_ratio,
    review_count,
    high_risk_count,
    cast(high_risk_count as double) / nullif(cast(txn_count as double), 0.0) as high_risk_ratio,
    cvc2_failure_count,
    ecommerce_or_token_count,
    distinct_merchants,
    distinct_regions,
    distinct_card_schemes,
    avg_merchant_distance_km,
    max_merchant_distance_km,
    max_cash_withdrawals_48h_eur,
    avg_network_risk_score,
    avg_issuer_fraud_score,
    txn_count_7d,
    total_amount_eur_7d,
    txn_count_30d,
    declined_count_30d,
    high_risk_count_30d,
    avg_daily_amount_eur_30d,
    total_amount_eur / nullif(avg_daily_amount_eur_30d, 0.0) as amount_to_30d_avg,
    max_merchant_distance_km_30d,
    fraud_count_30d,
    fraud_next_active_day
from rolling
