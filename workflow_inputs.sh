#!/bin/bash 
touch 'inputs.json'
jq -n '.ref = "main"' inputs.json
jq -n --arg hopsworks_domain "10.87.41.128" '.inputs.hopsworks_domain = $hopsworks_domain' inputs.json
jq -n --arg python_max_parallel 6 '.inputs.python_max_parallel = $python_max_parallel' inputs.json
jq -n --arg pyspark_max_parallel 4 '.inputs.pyspark_max_parallel = $pyspark_max_parallel' inputs.json
jq -n --arg labels "['e2e_small']" '.inputs.labels = $labels' inputs.json
jq -n --arg hopsworks_api_branch "main" '.inputs.hopsworks_api_branch = $hopsworks_api_branch' inputs.json
jq -n --arg loadtest_branch "main" '.inputs.loadtest_branch = $loadtest_branch' inputs.json

cat inputs.json