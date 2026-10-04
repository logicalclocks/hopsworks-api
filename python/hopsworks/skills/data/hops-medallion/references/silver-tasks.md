# Silver tasks: SQL and PySpark

Each task as a dbt model fragment (Trino SQL over `delta.<project>_featurestore.<fg>_<version>`) and as PySpark.
`{{ arrival }}` is the source's arrival column, `{{ key }}` its business key.
Chain them as CTEs in one model per silver table, in the order below.

## The window (every model)

```sql
with bronze as (
  select * from {{ source('bronze', 'crm_customers_1') }}
  {% if var('start_time', none) %}
  where {{ arrival }} >= from_iso8601_timestamp('{{ var("start_time") }}')
    and {{ arrival }} < from_iso8601_timestamp('{{ var("end_time") }}')
  {% endif %}
)
```

```python
bronze = fs.get_feature_group("crm_customers", version=1).read()  # HOPS_START_TIME/HOPS_END_TIME applied
```

## Third normal form (every layer)

A bronze export that repeats a customer's and a product's attributes on every order line is split into one table per entity, each keyed by what its columns depend on:

```sql
-- customers: customer_id -> name, city, country_code
select customer_id, max_by(name, _loaded_at) as name, max_by(city, _loaded_at) as city,
       max_by(country_code, _loaded_at) as country_code, max(_loaded_at) as updated_at
from bronze group by customer_id

-- products: product_id -> product_name, product_type_code
select distinct product_id, product_name, product_type_code from bronze

-- orders: order_id -> customer_id, ordered_at
select distinct order_id, customer_id, ordered_at from bronze

-- order_lines: (order_id, line_no) -> product_id, quantity, unit_price
select order_id, line_no, product_id, quantity, unit_price from bronze
```

`country_code -> country_name` and `product_type_code -> product_type_name` are lookups of their own (`countries`, `product_types`), so `customers` and `products` keep only the codes.
An order's total is not stored: it is computed from `order_lines` in gold.
Each table is still deduplicated, typed and validated with the tasks below, on its own key.

## deduplicate

```sql
, deduplicated as (
  select * from (
    select *, row_number() over (partition by {{ key }}, updated_at order by {{ arrival }} desc) as rn
    from bronze
  ) where rn = 1
)
```

```python
from pyspark.sql import Window, functions as F
w = Window.partitionBy("customer_id", "updated_at").orderBy(F.col("_loaded_at").desc())
df = df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
```

## cast_types

```sql
, typed as (
  select cast(customer_id as bigint) as customer_id,
         try_cast(signup_date as date) as signup_date,
         cast(amount as decimal(18, 2)) as amount,
         lower(active) in ('true', 'yes', '1', 'y') as active
  from deduplicated
)
```

`try_cast` turns an unparseable value into a null, which `validate` then counts; never let a bad value fail the whole window.

## standardize

```sql
, standardized as (
  select *, upper(trim(country)) as country_raw,
         coalesce(m.iso2, upper(trim(country))) as country
  from typed left join {{ ref('country_codes') }} m on upper(trim(typed.country)) in (m.iso2, m.iso3, upper(m.name))
)
```

Mapping tables (countries, currencies, channel names) are ephemeral models over a `values` list in `models/mappings/`, versioned with the code.
Not dbt seeds: a seed writes a table into the feature store schema outside the feature group API.

## handle_nulls

```sql
, cleaned as (
  select *, nullif(trim(email), '') as email_clean,
         coalesce(segment, 'unknown') as segment
  from standardized
)
```

## validate

Rows failing a rule go to `<table>_rejects` with the rule name, never dropped silently:

```sql
, checked as (
  select *, case
      when customer_id is null then 'missing_key'
      when amount < 0 then 'negative_amount'
      when not regexp_like(email_clean, '^[^@]+@[^@]+\.[^@]+$') then 'bad_email'
    end as reject_reason
  from cleaned
)
-- silver: where reject_reason is null; rejects: where reject_reason is not null
```

dbt data tests (`unique`, `not_null`, `accepted_values`, `relationships`) on the model gate the insert, as **hops-dbt** describes.

## mask_pii

```sql
, masked as (
  select *, to_hex(sha256(to_utf8(lower(email_clean) || '{{ env_var("PII_SALT") }}'))) as email_hash,
         regexp_replace(phone, '\d(?=\d{2})', '*') as phone_masked
  from checked
)
```

Drop the raw PII columns from the silver select list.
The salt is a secret (`hopsworks.get_secrets_api().create_secret("pii_salt", ...)`, or Account settings, Secrets), read by the runner with `get_secret("pii_salt").value` and passed to dbt as the `PII_SALT` environment variable, never written in the code or `system.yaml`.
A hash keeps the column joinable across tables; a mask keeps it readable for support; a column nobody needs is dropped.

## conform_entities

```sql
, crm as (select email_hash, customer_id as crm_id, name from {{ ref('crm_customers_clean') }}),
  shop as (select email_hash, account_id as shop_id, name from {{ ref('shop_accounts_clean') }}),
  conformed as (
    select coalesce(crm.email_hash, shop.email_hash) as email_hash, crm.crm_id, shop.shop_id,
           coalesce(crm.name, shop.name) as name
    from crm full outer join shop on crm.email_hash = shop.email_hash
  )
```

Match on a deterministic key where one exists; fuzzy matching (names, addresses) needs PySpark and a library, and its threshold is a decision recorded in `system.yaml`.

## surrogate_keys

```sql
, keyed as (
  select to_hex(sha256(to_utf8(coalesce(crm_id, '') || '|' || coalesce(shop_id, '')))) as customer_sk, *
  from conformed
)
```

A hash of the natural keys is stable across runs and windows, needs no sequence, and is the silver feature group's primary key.

## referential_checks

```sql
, orders_checked as (
  select o.*, c.customer_sk is null as orphan_customer
  from orders o left join {{ ref('customers') }} c on o.customer_sk = c.customer_sk
)
```

Orphans are flagged, not dropped: a fact can arrive before its dimension, and the next window may resolve it.
