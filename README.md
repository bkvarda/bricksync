## Bricksync
A tool to synchronize Delta and Iceberg tables between systems. 
Bricksync will programatically expand source catalogs and schemas, 
as well as sync Views and their dependencies. 
This is currently a WIP

### Sources
Current supported sources are:
- Databricks (Tables, MVs, STs, and Views)

### Targets
Current supported targets are:
- Snowflake

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
syncs:
  - source: managed.managed_delta.view_test
    source_provider: databricks
    target_provider: glue
  - source: managed.managed_delta.view_test
    source_provider: databricks
    target_provider: snowflake

target_format_preference: iceberg_preferred
```
You can also specify it without syncs.

To run a sync with syncs defined in config:
```
from bricksync import BrickSync
b = BrickSync.load("my.yaml").sync()
```
To define syncs inline:
```
b = BrickSync.load("my.yaml")
b.add_sync(source="my.uc", source_provider="databricks", target_provider="snowflake")
print(b.show_syncs())
b.sync()
```