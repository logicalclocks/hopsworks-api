# dbt Semantic Layer clients and BI tools

`dbt-sl-sdk` and the Arrow Flight SQL JDBC driver, with dbt's `{{ semantic_layer.query(...) }}` syntax, reach a semantic project with the Hopsworks host, an API key with the `FEATURESTORE` and `TRINO` scopes, and the semantic project's id as the environment id:

```bash
eval "$(hops semantic connect sales)"   # DBT_SL_GRAPHQL_URL_FORMAT, DBT_SL_ADBC_URL_FORMAT and, with a private CA, SSL_CERT_FILE; URLs on stderr
```

```python
info = sales.connection_info()          # environment_id, graphql_url, adbc_url, jdbc_url, env and ca_path
```

- **GraphQL** (`graphql_url`) serves metadata and `compileSql` only: `metrics`, `dimensions`, `measures`, `entities`, `savedQueries`, their paginated forms, `queryableGranularities` and `environmentInfo`.
- **Queries** run over Arrow Flight SQL (`adbc_url`, `jdbc_url`) or REST. `dbt-sl-sdk`'s `client.query()` and `client.dimension_values()` use ADBC and work; a query sent as a GraphQL operation (`createQuery`, `query`, `createDimensionValuesQuery`) is refused with a GraphQL validation error.

## BI tools (experimental)

On the same Flight SQL port, a statement without `{{ }}` is plain SQL over tables: catalog `semantic_layer`, schema the environment id (`semantic_layer."7"`), the table `metrics` (`metric_time__<grain>`, then every group-by, then every metric) and one table per saved query.
DBeaver, DataGrip and other JDBC/ODBC tools browse them; dbt's Tableau and Power BI connectors need the admin's dbt-compatible layout (Flight and `/api/graphql` on port 443 of one host).

```sql
SELECT customer__country, DATE_TRUNC('month', metric_time__day) AS m, SUM(revenue)
FROM semantic_layer."7".metrics
WHERE customer__country IN ('SE', 'NO')
GROUP BY 1, 2 HAVING SUM(revenue) > 100 ORDER BY 3 DESC LIMIT 50
```

- `SUM`, `AVG`, `MIN`, `MAX`, `COUNT` and `ANY_VALUE` around a metric column all mean the metric itself: `AVG(revenue)` is revenue, not an average of it. Never tell a user it is an average.
- One table per statement; group-bys in WHERE, metrics in HAVING; `DATE_TRUNC` (both argument orders), `EXTRACT` (ISODOW, not DOW) and lossless `CAST`s only.
- Refused with `INVALID_ARGUMENT: the BI tables do not support <construct>; ...`: joins, unions, subqueries, window functions, expressions over columns, `COUNT(*)`, metrics in WHERE, parameter markers.
- Recorded in the history with surface `BI`; never answered from exports.
