# perf-tests

## Usage

Create a `.env`:

```
TF_VAR_env=production
TF_VAR_cluster_template_id=gcp-io-optimized-v2
TF_VAR_cluster_mem_fleet=8G
TF_VAR_cluster_mem_es=16G
TF_VAR_stack_version=8.5.2
TF_VAR_stack_hash=e5ebf87b
TF_VAR_apikey="abcdefghijklm"
```

## Running

```
> ./oblt-perf run suites/suite1_perf02.py
```

