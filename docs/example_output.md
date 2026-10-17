# Example pipeline output

Captured from a local run: `python -m src.pipeline --all` then `python scripts/show_results.py` (synthetic data).

```
=== Pipeline runs (latest per pipeline/batch) ===
|pipeline          |batch_id |records_read|records_written|records_rejected|status |
|bronze.leases     |batch_001|1668        |1668           |0               |SUCCESS|
|bronze.maintenance|batch_001|1483        |1483           |0               |SUCCESS|
|bronze.payments   |batch_001|14618       |14616          |2               |SUCCESS|
|bronze.properties |batch_001|102         |102            |0               |SUCCESS|
|bronze.tenants    |batch_001|1503        |1503           |0               |SUCCESS|
|bronze.units      |batch_001|1061        |1061           |0               |SUCCESS|
|bronze.valuations |batch_001|592         |592            |0               |SUCCESS|
|silver.leases     |batch_001|1668        |1653           |13              |SUCCESS|
|silver.maintenance|batch_001|1483        |1477           |6               |SUCCESS|
|silver.payments   |batch_001|14616       |14582          |9               |SUCCESS|
|silver.properties |batch_001|102         |100            |0               |SUCCESS|
|silver.tenants    |batch_001|1503        |1498           |2               |SUCCESS|
|silver.units      |batch_001|1061        |1058           |3               |SUCCESS|
|silver.valuations |batch_001|592         |589            |3               |SUCCESS|
|bronze.leases     |batch_002|36          |36             |0               |SUCCESS|
|bronze.payments   |batch_002|66          |66             |0               |SUCCESS|
|bronze.tenants    |batch_002|39          |39             |0               |SUCCESS|
|bronze.valuations |batch_002|100         |100            |0               |SUCCESS|
|silver.leases     |batch_002|36          |35             |1               |SUCCESS|
|silver.payments   |batch_002|66          |65             |1               |SUCCESS|
|silver.tenants    |batch_002|39          |38             |1               |SUCCESS|
|silver.valuations |batch_002|100         |100            |0               |SUCCESS|
=== Quarantine reasons ===
|entity|batch_id |reason                 |count|
|units |batch_001|invalid_int:square_feet|1    |
|units |batch_001|unknown_property_id    |2    |
|entity |batch_id |reason       |count|
|tenants|batch_001|invalid_email|2    |
|tenants|batch_002|invalid_email|1    |
|entity|batch_id |reason                       |count|
|leases|batch_001|conflicting_duplicate_key    |4    |
|leases|batch_001|invalid_date:lease_start_date|2    |
|leases|batch_001|lease_end_before_start       |4    |
|leases|batch_001|negative_monthly_rent        |2    |
|leases|batch_001|unknown_tenant_id            |1    |
|leases|batch_002|unknown_unit_id              |1    |
|entity  |batch_id |reason                       |count|
|payments|batch_001|invalid_date:payment_due_date|2    |
|payments|batch_001|negative_amount_paid         |3    |
|payments|batch_001|unknown_lease_id             |4    |
|payments|batch_002|negative_amount_paid         |1    |
|entity    |batch_id |reason                     |count|
|valuations|batch_001|property_value_not_positive|2    |
|valuations|batch_001|unknown_property_id        |1    |
|entity     |batch_id |reason                   |count|
|maintenance|batch_001|completion_before_request|2    |
|maintenance|batch_001|negative_cost            |2    |
|maintenance|batch_001|unknown_unit_id          |2    |
|  entity| batch_id|     error_reason|count|
|payments|batch_001|malformed_csv_row|    2|
=== Silver row counts ===
  silver.properties                  100
  silver.units                      1058
  silver.tenants                    1533 (current: 1518)
  silver.leases                     1673
  silver.payments                  14602
  silver.valuations                  689
  silver.maintenance                1477
  silver.property_value_history      689 (current: 100)
=== Portfolio summary (gold.portfolio_summary) ===
  total_properties               100
  total_units                    1058
  occupied_units                 968
  vacant_units                   90
  scheduled_monthly_rent         3931221.17
  billed_rent                    3930737.96
  collected_rent                 3439451.14
  outstanding_rent               491286.82
  portfolio_value                549604000.00
  portfolio_value_12m_ago        523956000.00
  ttm_collected_rent             44451296.54
  ttm_maintenance_cost           2903786.29
  net_rent_after_maintenance     41547510.25
  estimated_noi                  28212121.31
  leases_expiring_30_days        52
  leases_expiring_90_days        172
  delinquent_tenants             95
  delinquent_balance             993221.90
  total_open_balance             1233506.18
  occupancy_rate                 91.49
  collection_rate                87.5
  avg_rent_per_occupied_unit     4061.18
  portfolio_value_growth_pct     4.9
  as_of_date                     2026-09-30
  reporting_month                2026-09
```

## Re-running batch_002 (idempotency check on the full dataset)

```
bronze.payments   before= 14682 after= 14682 OK
silver.leases     before=  1673 after=  1673 OK
silver.payments   before= 14602 after= 14602 OK
silver.tenants    before=  1533 after=  1533 OK   (current versions: 1518, one per tenant)
quarantine.payments before=  10 after=    10 OK
```
