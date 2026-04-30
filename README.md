# Pattern-recognition-and-machine-learning

## Install dependencies

```python
pip install -r requirements.txt
```

## Duckdb

https://duckdb.org/


## Data PREP - steps

1. Convert to Parquet Individually: Loop through your 10 heavy CSV files and run a simple COPY command to convert each one into a compressed Parquet file to eliminate CSV parsing lag.

2. Stack the Data: Use SELECT * FROM read_parquet('domain_folder/*.parquet', union_by_name = true) to virtually combine all the years into a single table.

3. Clean the Unified Domain: Run your SQL cleaning operations (handling NULLs, standardizing text, removing duplicates) on this single stacked view to ensure the cleaning logic is applied consistently across all years.

4. Basic EDA on the Clean Domains: Now that your three domains are clean and properly formatted, do your basic EDA on each domain to verify the standardizations worked.

5. Join the Domains: Join your heavy 10-year domain with the two lightweight domains to create a single, wide, denormalized table.

6. Advanced EDA: Run your final statistical EDA (frequency distributions, skewness, kurtosis) on the wide table to prepare for your final database normalization phase.