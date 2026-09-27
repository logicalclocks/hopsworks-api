## Python client micro-benchmarks

Single-process benchmarks of Python client code paths, without a cluster.
Each script stubs the network, so it measures the client's own work.

Run from `locust_benchmark/`, in an environment with the `hopsworks` client installed from the revision under test:

```bash
python -m client_benchmarks.deployment_schema --json after.json
```

To compare two revisions, install each in turn and write each run to its own `--json` file.

| Script | Measures |
| --- | --- |
| `deployment_schema` | Validating and encoding deployment request rows against a schema |
| `sql_dispatcher` | Online SQL feature vector reads through the client's task thread |
| `rest_feature_vectors` | Online REST feature vector decoding and assembly, by position and by name |
| `rest_transport` | Calling-thread CPU per online store REST request, through Requests and through urllib3 |
