# Custom target usage

- Run `meltano add --custom loader target-iceberg`
- Add pip_url: `git+https://github.com/taeefnajib/target-iceberg.git`
- Add the config properties manually in meltano.yml:

```
config:
  add_record_metadata: true
```
- IMPORTANT: ensure that aws credentials are set correctly (this target uses DynamoDB as a catalog)