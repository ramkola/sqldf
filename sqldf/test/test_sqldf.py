from sqldf import sqldf

from pyspark import SparkContext
from pyspark import RDD
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from sqldf import templating
import os


raw_df = [{'name': 'Rigo', 'age': 33}, {'name': 'Bruno', 'age': 33}]

pyspark_df = sqldf.sql('select * from dataframe', raw_df)
pyspark_df.show()

# Update age for rigo to 34.
pyspark_df_age_upd = sqldf.sql(
	"""SELECT name,
			  CASE
				WHEN name = '{{ name }}' THEN 34
				ELSE age
			  END AS age
		 FROM dataframe_two""",
	pyspark_df,
	table='dataframe_two',
	name='Rigo')

pyspark_df_age_upd.show()

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

