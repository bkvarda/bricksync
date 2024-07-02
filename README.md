## Bricksync
A tool to synchronize Delta and Iceberg tables between systems. Bricksync will programatically expand source catalogs and schemas, as well as sync Views and their dependencies. This is currently a WIP

### Catalogs
Current supported catalogs are:
- Databricks (Tables, MVs, STs, and Views)
- Snowflake (Views and Tables)
- Glue (Tables)

### Usage
Create a config file that looks like:
```
providers:
  - glue:
      provider: glue
      type: target
      configuration:
        region_name: us-west-2
        profile_name: my-profile
  - snowflake:
      provider: snowflake
      type: target
      configuration:
        account: 12345-12345
        warehouse: xmallwh
  - databricks:
       provider: databricks
       type: source
       configuration:
         profile: dnb-sandbox
         otherkey: othervalue
```

Sync from one Catalog to another using `sync()`. For example, the below syncs a Delta UniForm table to Glue, then syncs that Glue Iceberg table to Snowflake as an Iceberg table: 
```
from bricksync import BrickSync
b = BrickSync.load("my.yaml")
b.sync('databricks', 'external.external_delta.glue_test', 'glue', 'external.external_delta.glue_test')
b.sync('glue', 'external_delta.glue_test', 'snowflake', 'external.external_delta.glue_test')
```
### Catalog-specific helpers
Catalogs have helpers that enable you to perform tasks that might be useful as part of syncing operations.
#### Databricks
```
b = BrickSync.load("my.yaml")
databricks = b.get_provider("databricks")
# This will synchronously generate UniForm iceberg metadata for some table
databricks.generate_iceberg_metadata('my.uniform.table')
```
#### Snowflake
```
b = BrickSync.load("my.yaml")
sf = b.get_provider("snowflake")
sf.create_catalog_integration('OBJECT_CATALOG')

volume = sf.create_external_volume(name = 'managed', 
                                   storage_base_url='s3://mys3bucket/metastore/',
                                   storage_provider='s3',
                                   storage_aws_role_arn='arn:aws:iam::123456789101112:role/my-aws-role',)
                                   ```
