-- The shape the backend emits TODAY for a windowed aggregate on the ROOT feature
-- group (ConstructorController.buildAggregateRunningCteNode, pinned by
-- TestConstructorControllerCollect.testAggregatePitRunningDerivedTable).
--
-- The finite RANGE frame is the near-quadratic operator review PIT-P1 removed from
-- the direct-join timeline: Spark re-aggregates the frame buffer for every row.
-- Kept here as the baseline the cumulative rewrite (PERF-2026-08-1) is measured and
-- compared against.
SELECT "fg0"."pk",
       SUM("fg0"."amount") OVER (
         PARTITION BY "fg0"."pk"
         ORDER BY EPOCH_US("fg0"."ts")
         RANGE BETWEEN 3600000000 PRECEDING AND CURRENT ROW) AS "amount_sum"
FROM "fs1"."fg1_1" AS "fg0"
ORDER BY 1, 2
