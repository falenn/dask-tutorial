#!/usr/bin/env python

import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from matplotlib import pyplot as plt


df = dd.read_csv('../data/nyc-parking-tickets/*2017.csv')

# Type conversions
df["Date.First.Observed"] = pd.to_datetime(df["Date.First.Observed"])
df["Vehichle.Expiration.Date"] = pd.to_datetime(df["Vehicle.Expiration.Date"])
df["NTA"] = pd.to_object(df["NTA"])

print(F"Imported data types: {df.dtypes}")

print(F"Num Rows: {len(df.index)}")

missing_values = df.isnull().sum()
print(F"Missing values: {missing_values}")

missing_count = ((missing_values / df.index.size) * 100)
print(F"Missing count: {missing_count}")

with ProgressBar():
  missing_count_pct = missing_count.compute()
print(F"missing count pct {missing_count_pct}")
