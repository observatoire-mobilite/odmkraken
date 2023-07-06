import dagster

busdata_partition = dagster.DailyPartitionsDefinition(start_date="2020-01-01")