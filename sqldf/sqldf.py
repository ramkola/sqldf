from pyspark import SparkContext
from pyspark import RDD
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
import os


os.environ["SPARK_HOME"] = "/usr/local/spark/"
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"

# Set spark context to be used throwout
sc = SparkContext("local", 'sqldf')
sqlContext = SQLContext(sc)


def get_column_names(dataframe) -> list:
	"""
	Returns the column names for the RAW dataframe
	
	:param dataframe:
		RAW, pyspark, or pandas dataframe.
	"""
	columns = []
	for row in dataframe:
		columns = [k for (k, v) in row.items()]
	return columns


def convert_to_row(list_of_dictionaries: list) -> RDD:
	"""
	Converts list of dictionaries to pyspark suppored RDD format.
	
	:param list_of_dictionaries:
		A list of dictionaries. E.g.: [{'name': 'Rigo', 'age': 33}, {...}]
	"""
	
	return sc.parallelize(list_of_dictionaries).map(lambda _: [
		(v) for (k, v) in _.items()])


def register_pyspark_df(pyspark_df: DataFrame):
	"""
	Register the dataframe as a table so it can be query using sql
	
	:param pyspark_df:
		A pyspark DataFrame
	"""
	
	sqlContext.registerDataFrameAsTable(pyspark_df, 'dataframe')


def convert_to_pyspark_df(dataframe) -> DataFrame:
	"""
	Converts the DataFrame into a pyspark DataFrame
	
	:param dataframe:
		A RAW, pyspark, or pandas dataframe
	"""
	
	# Is the DataFrame a list of dictionaries? RAW?
	if type(dataframe) is DataFrame:
		return dataframe
	elif type(dataframe) is list:
		for row in dataframe:
			if type(row) is dict:
				columns = get_column_names(dataframe)
					rdd: RDD = convert_to_row(dataframe)
					return sqlContext.createDataFrame(rdd, columns)
	elif type(dataframe) == 'pandas.core.frame.DataFrame':
		return sqlContext.createDataFrame(dataframe)
	else:
		raise ValueError(f'Invalid DataFrame type: {type(dataframe)}')


def sql(query: str, dataframe) -> DataFrame:
	"""
	Returns a pyspark Dataframe 
	Example (RAW DataFrame): 
		dataframe = [{'Name': 'Rigo', 'age': 3}, {'Name': 'Lindsay', 'age': 5}]
		sql('select Name, sum(age) from dataframe group by Name', dataframe).show()
			
	:param query: 
		The query to run against the dataframe
	:param dataframe:
		A RAW, pyspark, or pandas dataframe		
	"""
	
	pyspark_df: DataFrame = convert_to_pyspark_df(dataframe)
	return register_pyspark_df(pyspark_df).sqlContext.sql(query)

