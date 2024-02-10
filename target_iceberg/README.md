# Custom target usage

- Run `meltano add --custom loader target-iceberg`
- Add pip_url: `git+https://github.com/SidetrekAI/target-iceberg`
- Add the config properties manually in meltano.yml:

```
config:
  add_record_metadata: true
```

## Make sure to add these environment variables to the instance where meltano is running
Note: The values are just examples.

```
export PYICEBERG_CATALOG__DEFAULT__URI=http://iceberg-rest:8181
export PYICEBERG_CATALOG__DEFAULT__S3__ACCESS_KEY_ID=username
export PYICEBERG_CATALOG__DEFAULT__S3__SECRET_ACCESS_KEY=password
```