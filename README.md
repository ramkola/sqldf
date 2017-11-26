# sqldf
Library for quering DataFrames using SQL.

## Overview
SQL DataFrame is a library used for querying multiple types of DataFrame using ANSI SQL syntax. Dataframes supported are: 
1. RAW: usually used so the user doesn’t need to have pandas installed. Example of a RAW "DataFrame":
```python
[{'name': 'Rigo', 'age': 33}, {'name': 'Bruno', 'age': 33}]
```

2. pandas DataFrame.

3. pyspark DataFrame: sqldf uses pyspark DataFrame in the background to process big data. Note, that the DataFrame return will be a pyspark DataFrame

## How to use sqldf
```python
from sqldf import sqldf

raw_df = [{'name': 'Rigo', 'age': 33}, {'name': 'Bruno', 'age': 33}]

# Table name defaults to dataframe. You can change the table name by especifying table in sqldf.sql()
query = ‘SELECT * FROM dataframe’

pyspark_df = sqldf.sql(query, raw_df)

pyspark_df.show()
```

### How to update columns
sqldf doesn't support update statements yet (future release), so this is how you would update a column value in a DataFrame.
Continuation of the example above:
```python
# Update age for rigo to 34. Note that we changed the table name here.
pyspark_df_age_upd = sqldf.sql(
	"""SELECT name,
			  CASE
				WHEN name = '{{ name }}' THEN 34
				ELSE age
			  END AS age
		 FROM dataframe_two""",
	pyspark_df,
	table='dataframe_two'
	name='Rigo')

pyspark_df_age_upd.show()
```

### How to merge two dataframes
```python
# Merge two tables: dataframe and new table. Note that dataframe was already added on the first example. dataframe2 is added now.
# If you don't provide table name it will dataframe will be overwritten with this new query.
sqldf.sql(
	"""
	SELECT * FROM dataframe t1
	UNION
	SELECT * FROM dataframe2 t2
	""",
	pyspark_df_age_upd,
	table='dataframe2'
).show()
```

