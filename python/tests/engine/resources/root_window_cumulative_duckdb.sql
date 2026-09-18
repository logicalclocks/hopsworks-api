-- PERF-2026-08-1 target shape: the ROOT windowed aggregate on cumulative frames only.
--
-- This is the same non-subtractive two-bucket decomposition the joined timeline uses
-- (ConstructorController.buildWindowedAggregateTimelineSubquery, review ENT-PIT-1),
-- specialised for the case where the spine IS the source. Two consequences make it
-- cheaper than routing the root through the joined builder:
--
--   * one scan, not two. A root row already carries both the marker identity and the
--     value, so ONE scan CROSS JOINed with the 2-row role table emits the marker and
--     its lower-boundary probe. The joined builder needs a separate source branch and
--     a per-entity bounds relation; here source range == spine range by construction,
--     so pruning is vacuous and omitted.
--   * 2R union rows instead of 2R + S.
--
-- Frames are RANGE, not ROWS, matching cumulativeWindow(): rows sharing an ordinal are
-- peers and must all fall inside [t - w, t], which a ROWS frame would split
-- arbitrarily.
--
-- Buckets are exactly w microseconds wide, so floor((t-w)/w) = floor(t/w) - 1 always:
-- [t-w, t] spans two ADJACENT buckets. The marker's forward segment covers
-- [start of its bucket, t] and the probe's backward segment covers
-- [t-w, end of its bucket]. Adjacent and disjoint, so each in-window value is added
-- exactly once and nothing is ever subtracted.
WITH "scan" AS (
  SELECT "pk" AS "e0",
         EPOCH_US("ts") AS "ord",
         ROW_NUMBER() OVER (PARTITION BY "pk" ORDER BY EPOCH_US("ts")) AS "seq",
         "amount"
  FROM "fs1"."fg1_1"
), "tl" AS (
  SELECT "s"."e0",
         CASE WHEN "r"."role" = 1 THEN "s"."ord" ELSE "s"."ord" - 3600000000 END AS "ord",
         "r"."role" AS "marker",
         "s"."seq",
         CAST(FLOOR(
           (CASE WHEN "r"."role" = 1 THEN "s"."ord" ELSE "s"."ord" - 3600000000 END)
           / 3600000000) AS BIGINT) AS "bucket",
         -- values ride the marker row; the probe is a read point only
         CASE WHEN "r"."role" = 1 THEN "s"."amount" END AS "sv0",
         CASE WHEN "r"."role" = 1 AND "s"."amount" IS NOT NULL THEN 1 END AS "cv0"
  FROM "scan" AS "s"
  CROSS JOIN (SELECT 1 AS "role" UNION ALL SELECT 2 AS "role") AS "r"
), "w" AS (
  SELECT "e0", "marker", "seq",
         SUM("sv0") OVER (PARTITION BY "e0", "bucket" ORDER BY "ord"
           RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sf0",
         SUM("sv0") OVER (PARTITION BY "e0", "bucket" ORDER BY "ord" DESC
           RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sb0",
         SUM("cv0") OVER (PARTITION BY "e0", "bucket" ORDER BY "ord"
           RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "cf0",
         SUM("cv0") OVER (PARTITION BY "e0", "bucket" ORDER BY "ord" DESC
           RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "cb0"
  FROM "tl"
)
SELECT "e0" AS "pk",
       -- an empty or all-NULL window sums to 0 through the segment merge, not to the
       -- finite frame's NULL; the merged non-null count restores it
       CASE WHEN COALESCE(MAX(CASE WHEN "marker" = 1 THEN "cf0" END), 0)
               + COALESCE(MAX(CASE WHEN "marker" = 2 THEN "cb0" END), 0) > 0
            THEN COALESCE(MAX(CASE WHEN "marker" = 1 THEN "sf0" END), 0)
               + COALESCE(MAX(CASE WHEN "marker" = 2 THEN "sb0" END), 0)
       END AS "amount_sum"
FROM "w"
WHERE "marker" >= 1
GROUP BY "e0", "seq"
ORDER BY 1, 2
