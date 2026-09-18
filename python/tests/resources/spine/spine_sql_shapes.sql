-- ASOF root_pruned
SELECT "spine"."date" "date", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ASOF LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF root_kept
SELECT "spine"."date" "date", "fg0"."pm25" "pm25", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ASOF LEFT JOIN "test_proj_featurestore"."air_quality_1" "fg0" ON "spine"."country" = "fg0"."country" AND "spine"."city" = "fg0"."city" AND "spine"."street" = "fg0"."street" AND "spine"."date" >= "fg0"."date"
ASOF LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF child_filter
SELECT "spine"."date" "date", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ASOF LEFT JOIN (SELECT *
FROM "test_proj_featurestore"."weather_1" "fg1"
WHERE "fg1"."temperature_2m_mean" > 0) "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF child_filter_dedupe
SELECT "spine"."date" "date", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ASOF LEFT JOIN (SELECT *
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY "fg1"."city", "fg1"."date" ORDER BY "fg1"."station") "pit_dedupe_hopsworks"
FROM (SELECT *
FROM "test_proj_featurestore"."weather_1" "fg1"
WHERE "fg1"."temperature_2m_mean" > 0) "fg1") "fg1"
WHERE "fg1"."pit_dedupe_hopsworks" = 1) "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF child_filter_dropped
SELECT "spine"."date" "date", CAST(NULL AS DOUBLE) "temperature_2m_mean", CAST(NULL AS DOUBLE) "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF root_filter
SELECT "spine"."date" "date", "fg0"."pm25" "pm25", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ASOF LEFT JOIN (SELECT *
FROM "test_proj_featurestore"."air_quality_1" "fg0"
WHERE "fg0"."pm25" > 0) "fg0" ON "spine"."country" = "fg0"."country" AND "spine"."city" = "fg0"."city" AND "spine"."street" = "fg0"."street" AND "spine"."date" >= "fg0"."date"
ASOF LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF feature_age_star
SELECT "spine"."date" "date", CASE WHEN "fg1"."date" >= "spine"."date" - INTERVAL '86400' SECOND THEN "fg1"."temperature_2m_mean" ELSE NULL END "temperature_2m_mean", CASE WHEN "fg1"."date" >= "spine"."date" - INTERVAL '86400' SECOND THEN "fg1"."wind_speed_10m_max" ELSE NULL END "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ASOF LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF all_skipped
SELECT "spine"."date" "date", CAST(NULL AS DOUBLE) "temperature_2m_mean", CAST(NULL AS DOUBLE) "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF dedupe_no_filter
SELECT "spine"."date" "date", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ASOF LEFT JOIN (SELECT *
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY "fg1"."city", "fg1"."date" ORDER BY "fg1"."station") "pit_dedupe_hopsworks"
FROM "test_proj_featurestore"."weather_1" "fg1") "fg1"
WHERE "fg1"."pit_dedupe_hopsworks" = 1) "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF passthrough_label
SELECT "spine"."date" "date", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max", "spine"."label" "label"
FROM "__hopsworks_spine_3f9a" "spine"
ASOF LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF epoch_child
SELECT "spine"."date" "date", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ASOF LEFT JOIN "test_proj_featurestore"."weather_epoch_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= epoch_ms("fg1"."date")
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF epoch_child_feature_age
SELECT "spine"."date" "date", CASE WHEN epoch_ms("fg1"."date") >= "spine"."date" - INTERVAL '86400' SECOND THEN "fg1"."temperature_2m_mean" ELSE NULL END "temperature_2m_mean", CASE WHEN epoch_ms("fg1"."date") >= "spine"."date" - INTERVAL '86400' SECOND THEN "fg1"."wind_speed_10m_max" ELSE NULL END "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ASOF LEFT JOIN "test_proj_featurestore"."weather_epoch_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= epoch_ms("fg1"."date")
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF epoch_root_feature_age
SELECT "spine"."date" "date", CASE WHEN epoch_ms(CAST("fg1"."date" AS TIMESTAMP)) >= "spine"."date" - 86400000 THEN "fg1"."temperature_2m_mean" ELSE NULL END "temperature_2m_mean", CASE WHEN epoch_ms(CAST("fg1"."date" AS TIMESTAMP)) >= "spine"."date" - 86400000 THEN "fg1"."wind_speed_10m_max" ELSE NULL END "wind_speed_10m_max"
FROM "__hopsworks_spine_e90c" "spine"
ASOF LEFT JOIN "test_proj_featurestore"."air_quality_epoch_1" "fg0" ON "spine"."country" = "fg0"."country" AND "spine"."city" = "fg0"."city" AND "spine"."street" = "fg0"."street" AND "spine"."date" >= "fg0"."date"
ASOF LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= epoch_ms(CAST("fg1"."date" AS TIMESTAMP))
ORDER BY "spine"."__hopsworks_spine_row_id";

-- ASOF complex_typed_nulls
SELECT "spine"."date" "date", CAST(NULL AS FLOAT[]) "wind_dir", CAST(NULL AS STRUCT(label VARCHAR, index INTEGER)) "station_meta"
FROM "__hopsworks_spine_3f9a" "spine"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- WINDOWED root_pruned
WITH lookup_fg1 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg1"."date" DESC) "pit_rank_hopsworks"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date") (SELECT "spine"."date" "date", "lookup_fg1"."temperature_2m_mean" "temperature_2m_mean", "lookup_fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "lookup_fg1" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg1"."__hopsworks_spine_row_id" AND "lookup_fg1"."pit_rank_hopsworks" = 1
ORDER BY "spine"."__hopsworks_spine_row_id");

-- WINDOWED root_kept
WITH lookup_fg0 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg0"."pm25" "pm25", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg0"."date" DESC) "pit_rank_hopsworks"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "test_proj_featurestore"."air_quality_1" "fg0" ON "spine"."country" = "fg0"."country" AND "spine"."city" = "fg0"."city" AND "spine"."street" = "fg0"."street" AND "spine"."date" >= "fg0"."date"), lookup_fg1 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg1"."date" DESC) "pit_rank_hopsworks"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date") (SELECT "spine"."date" "date", "lookup_fg0"."pm25" "pm25", "lookup_fg1"."temperature_2m_mean" "temperature_2m_mean", "lookup_fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "lookup_fg0" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg0"."__hopsworks_spine_row_id" AND "lookup_fg0"."pit_rank_hopsworks" = 1
LEFT JOIN "lookup_fg1" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg1"."__hopsworks_spine_row_id" AND "lookup_fg1"."pit_rank_hopsworks" = 1
ORDER BY "spine"."__hopsworks_spine_row_id");

-- WINDOWED child_filter
WITH lookup_fg1 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg1"."date" DESC) "pit_rank_hopsworks"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN (SELECT *
FROM "test_proj_featurestore"."weather_1" "fg1"
WHERE "fg1"."temperature_2m_mean" > 0) "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date") (SELECT "spine"."date" "date", "lookup_fg1"."temperature_2m_mean" "temperature_2m_mean", "lookup_fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "lookup_fg1" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg1"."__hopsworks_spine_row_id" AND "lookup_fg1"."pit_rank_hopsworks" = 1
ORDER BY "spine"."__hopsworks_spine_row_id");

-- WINDOWED child_filter_dedupe
WITH lookup_fg1 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg1"."date" DESC, "fg1"."station") "pit_rank_hopsworks"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN (SELECT *
FROM "test_proj_featurestore"."weather_1" "fg1"
WHERE "fg1"."temperature_2m_mean" > 0) "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date") (SELECT "spine"."date" "date", "lookup_fg1"."temperature_2m_mean" "temperature_2m_mean", "lookup_fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "lookup_fg1" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg1"."__hopsworks_spine_row_id" AND "lookup_fg1"."pit_rank_hopsworks" = 1
ORDER BY "spine"."__hopsworks_spine_row_id");

-- WINDOWED child_filter_dropped
SELECT "spine"."date" "date", CAST(NULL AS DOUBLE) "temperature_2m_mean", CAST(NULL AS DOUBLE) "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- WINDOWED root_filter
WITH lookup_fg0 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg0"."pm25" "pm25", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg0"."date" DESC) "pit_rank_hopsworks"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN (SELECT *
FROM "test_proj_featurestore"."air_quality_1" "fg0"
WHERE "fg0"."pm25" > 0) "fg0" ON "spine"."country" = "fg0"."country" AND "spine"."city" = "fg0"."city" AND "spine"."street" = "fg0"."street" AND "spine"."date" >= "fg0"."date"), lookup_fg1 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg1"."date" DESC) "pit_rank_hopsworks"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date") (SELECT "spine"."date" "date", "lookup_fg0"."pm25" "pm25", "lookup_fg1"."temperature_2m_mean" "temperature_2m_mean", "lookup_fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "lookup_fg0" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg0"."__hopsworks_spine_row_id" AND "lookup_fg0"."pit_rank_hopsworks" = 1
LEFT JOIN "lookup_fg1" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg1"."__hopsworks_spine_row_id" AND "lookup_fg1"."pit_rank_hopsworks" = 1
ORDER BY "spine"."__hopsworks_spine_row_id");

-- WINDOWED feature_age_star
WITH lookup_fg1 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg1"."date" DESC) "pit_rank_hopsworks"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date" AND "fg1"."date" >= "spine"."date" - INTERVAL '86400' SECOND) (SELECT "spine"."date" "date", "lookup_fg1"."temperature_2m_mean" "temperature_2m_mean", "lookup_fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "lookup_fg1" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg1"."__hopsworks_spine_row_id" AND "lookup_fg1"."pit_rank_hopsworks" = 1
ORDER BY "spine"."__hopsworks_spine_row_id");

-- WINDOWED all_skipped
SELECT "spine"."date" "date", CAST(NULL AS DOUBLE) "temperature_2m_mean", CAST(NULL AS DOUBLE) "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
ORDER BY "spine"."__hopsworks_spine_row_id";

-- WINDOWED dedupe_no_filter
WITH lookup_fg1 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg1"."date" DESC, "fg1"."station") "pit_rank_hopsworks"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date") (SELECT "spine"."date" "date", "lookup_fg1"."temperature_2m_mean" "temperature_2m_mean", "lookup_fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "lookup_fg1" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg1"."__hopsworks_spine_row_id" AND "lookup_fg1"."pit_rank_hopsworks" = 1
ORDER BY "spine"."__hopsworks_spine_row_id");

-- WINDOWED passthrough_label
WITH lookup_fg1 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg1"."date" DESC) "pit_rank_hopsworks"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= "fg1"."date") (SELECT "spine"."date" "date", "lookup_fg1"."temperature_2m_mean" "temperature_2m_mean", "lookup_fg1"."wind_speed_10m_max" "wind_speed_10m_max", "spine"."label" "label"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "lookup_fg1" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg1"."__hopsworks_spine_row_id" AND "lookup_fg1"."pit_rank_hopsworks" = 1
ORDER BY "spine"."__hopsworks_spine_row_id");

-- WINDOWED epoch_child
WITH lookup_fg1 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg1"."date" DESC) "pit_rank_hopsworks"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "test_proj_featurestore"."weather_epoch_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= timestamp_millis("fg1"."date")) (SELECT "spine"."date" "date", "lookup_fg1"."temperature_2m_mean" "temperature_2m_mean", "lookup_fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "lookup_fg1" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg1"."__hopsworks_spine_row_id" AND "lookup_fg1"."pit_rank_hopsworks" = 1
ORDER BY "spine"."__hopsworks_spine_row_id");

-- WINDOWED epoch_child_feature_age
WITH lookup_fg1 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg1"."date" DESC) "pit_rank_hopsworks"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "test_proj_featurestore"."weather_epoch_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= timestamp_millis("fg1"."date") AND timestamp_millis("fg1"."date") >= "spine"."date" - INTERVAL '86400' SECOND) (SELECT "spine"."date" "date", "lookup_fg1"."temperature_2m_mean" "temperature_2m_mean", "lookup_fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_3f9a" "spine"
LEFT JOIN "lookup_fg1" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg1"."__hopsworks_spine_row_id" AND "lookup_fg1"."pit_rank_hopsworks" = 1
ORDER BY "spine"."__hopsworks_spine_row_id");

-- WINDOWED epoch_root_feature_age
WITH lookup_fg0 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg0"."date" DESC) "pit_rank_hopsworks"
FROM "__hopsworks_spine_e90c" "spine"
LEFT JOIN "test_proj_featurestore"."air_quality_epoch_1" "fg0" ON "spine"."country" = "fg0"."country" AND "spine"."city" = "fg0"."city" AND "spine"."street" = "fg0"."street" AND "spine"."date" >= "fg0"."date" AND "fg0"."date" >= "spine"."date" - 86400000), lookup_fg1 AS (SELECT "spine"."__hopsworks_spine_row_id" "__hopsworks_spine_row_id", "fg1"."temperature_2m_mean" "temperature_2m_mean", "fg1"."wind_speed_10m_max" "wind_speed_10m_max", ROW_NUMBER() OVER (PARTITION BY "spine"."__hopsworks_spine_row_id" ORDER BY "fg1"."date" DESC) "pit_rank_hopsworks"
FROM "__hopsworks_spine_e90c" "spine"
LEFT JOIN "test_proj_featurestore"."weather_1" "fg1" ON "spine"."city" = "fg1"."city" AND "spine"."date" >= unix_millis(CAST("fg1"."date" AS TIMESTAMP)) AND unix_millis(CAST("fg1"."date" AS TIMESTAMP)) >= "spine"."date" - 86400000) (SELECT "spine"."date" "date", "lookup_fg1"."temperature_2m_mean" "temperature_2m_mean", "lookup_fg1"."wind_speed_10m_max" "wind_speed_10m_max"
FROM "__hopsworks_spine_e90c" "spine"
LEFT JOIN "lookup_fg0" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg0"."__hopsworks_spine_row_id" AND "lookup_fg0"."pit_rank_hopsworks" = 1
LEFT JOIN "lookup_fg1" ON "spine"."__hopsworks_spine_row_id" = "lookup_fg1"."__hopsworks_spine_row_id" AND "lookup_fg1"."pit_rank_hopsworks" = 1
ORDER BY "spine"."__hopsworks_spine_row_id");

-- WINDOWED complex_typed_nulls
SELECT "spine"."date" "date", CAST(NULL AS ARRAY<FLOAT>) "wind_dir", CAST(NULL AS STRUCT<label: STRING, index: INT>) "station_meta"
FROM "__hopsworks_spine_3f9a" "spine"
ORDER BY "spine"."__hopsworks_spine_row_id";

