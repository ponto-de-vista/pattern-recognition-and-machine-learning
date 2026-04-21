import pandas as pd
import dask.dataframe as dd

# Load the CSV file
df = dd.read_csv('.\despesas-2008\despesas-2008.csv',
                 encoding='latin1',
                 on_bad_lines='skip',
                 sep=';',
                 assume_missing=True,
                 low_memory=False)

# View the first 5 rows
print(df.head())
print(len(df))