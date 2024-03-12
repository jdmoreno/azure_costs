load-reference-data

# Summary
Process to load Azure cost data from CSV to DB.
Uses _reference_data_ and _resource_groups_ tables to enrich Azure cost raw data to provide cost data per platform and per application.

Suggestion: Backup _cost_data_ table before adding new Azure CSV costs files
```sql
INSERT INTO public.cost_data_temp
SELECT * FROM public.cost_data;
```

## Arguments:
- '--files', '-f': Comma-delimited list of CSV files. Required. 
- '--config', '-c': Path to configuration file.  default='./resources/properties.toml'. Optional
- '--destination', '-d', 'Destination table rs for resource_groups, rd for reference_data, cd for cost data'. Required. choices=['rd', 'rs', 'cd']

## Usage

To load cost data:
```commandline
load-reference-data.py -f "./input/cost.data/*.csv" -d cd
```

To load reference data
```commandline
load-reference-data.py -f "./input/reference_data/export_reference_data.csv" -d rd
```

To load resource groups
```commandline
load-reference-data.py -f "./input/resource_groups/export_resource_groups.csv" -d rs
```

## Configuration
properties.toml

``` properties
[DB_CONFIG]
host = "localhost"
port = 55432
dbname = "azure-costs"
user = "postgres"
password = "changeme"

```

## Reference Data

### resource_groups
This table holds the relationship between _subscription_name_, _resource_group_ and Affix.
Columns are the unique combination of _subscription_name_, _resource_group_ from Azure cost raw data.  
Column _Affix_ It is populated manually.

## reference_data
Contains lookups to enrich Azure cost raw data
AREAS: Key: Affix, Value: Area
ENVIRONMENTS: Key: Azure Subscription, Value: Environment
CAPABILITIES: Key: Affix, Value: Capability. This is populated only for EPS

## Validation mode
In validation mode the utility will write 4 CSV files
./output/missing_subs_rg.csv : _Subscriptions_ and _Resource Groups_ entries not in resource_groups
./output/missing_areas.csv : _AREAS_ and _Affixes_ entries not in reference_data
./output/missing_areas.csv : _CAPABILITIES_ and _Affixes_ entries not in reference_data
./output/missing_areas.csv : _ENVIRONMENTS_ and _subscription_name_ entries not in reference_data

# Query to export data to excel
```sql
SELECT cost_period, area, capability, resource_type, environment, SUM(cost_c) 
FROM public.cost_data
GROUP BY cost_period, area, capability, resource_type, environment
ORDER BY cost_period ASC, area ASC, capability ASC, resource_type ASC, environment ASC
```