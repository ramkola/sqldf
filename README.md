# sqldf
Library for quering DataFrames using SQL.

## Overview
SQL DataFrame is a library used for querying multiple types of DataFrame using ANSI SQL syntax. Dataframes supported are: 
1. RAW: usually used so the user doesn’t need to have pandas installed. Example of a RAW "DataFrame":
```python
[{‘name’: ‘Rigo’}, {‘age’: 33}, {‘name’: ‘Bruno’}, {‘age’: 33}]
```

2. pandas DataFrame.

3. pyspark DataFrame: sqldf uses pyspark DataFrame in the background to process big data. Note, that the DataFrame return will be a pyspark DataFrame

## How to use sqldf
```python
from sqldf import *

raw_df = [{‘name’: ‘Rigo’}, {‘age’: 33}, {‘name’: ‘Bruno’}, {‘age’: 33}]

query = ‘SELECT * 
		   FROM dataframe’

pyspark_df = sql(query, raw_df)

pyspark_df.show()
```
