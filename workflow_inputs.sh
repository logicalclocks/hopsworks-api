#!/bin/bash 
touch 'inputs.yaml'
yq '.ref = "main"' -i inputs.yaml
yq '.inputs.python_max_parallel = 6' -i inputs.yaml
yq '.inputs.pyspark_max_parallel = 4' -i inputs.yaml
hopsworks_domain="10.87.41.128" yq '.inputs.hopsworks_domain = strenv(hopsworks_domain)' -i inputs.yaml
labels="['e2e_small']" yq  '.inputs.labels = $labels' -i inputs.yaml
hopsworks_api_branch="main" yq  '.inputs.hopsworks_api_branch = $hopsworks_api_branch' -i inputs.yaml
loadtest_branch="main" yq '.inputs.loadtest_branch = $loadtest_branch' -i inputs.yaml

yq -o=json inputs.yaml > inputs.json
cat inputs.json