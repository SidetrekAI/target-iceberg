# Custom target usage

- Add `ICEBERG_REST_CATALOG_URI` environment variable to the instance running this target
- Run `meltano add --custom loader target-iceberg`
- Add pip_url: `git+https://github.com/SidetrekAI/target-iceberg`
- Add the config properties manually in meltano.yml:

```
config:
  add_record_metadata: true
```
