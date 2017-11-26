from pyspark import SparkContext
from pyspark import RDD
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
import os


os.environ["SPARK_HOME"] = "/usr/local/spark/"
os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"

# Set spark context to be used throwout 
sc = SparkContext("local", 'sqldf')
sqlContext = SQLContext(sc)


def get_column_names(dataframe) -> list:
    columns = []
    for row in dataframe:
        columns = [k for (k, v) in row.items()]
    return columns


def convert_to_row(list_of_dictionaries: list) -> RDD:

    return sc.parallelize(list_of_dictionaries).map(lambda _: [(v) for (k, v) in _.items()])


def register_pyspark_df(pyspark_dataframe: DataFrame):
    """Register the dataframe as a table so it can be query using sql"""

    sqlContext.registerDataFrameAsTable(pyspark_dataframe, 'dataframe')


def convert_to_pyspark_df(dataframe) -> DataFrame:
    """Converts the DataFrame into a pyspark DataFarame"""

    # Is the DataFrame a list of dictionaries? RAW?
    if type(dataframe) is list:
        for row in dataframe:
            if type(row) is dict:
                columns = get_column_names(dataframe)
                rdd: RDD = convert_to_row(dataframe)
                return sqlContext.createDataFrame(rdd, columns)
    elif type(dataframe) == 'pandas.core.frame.DataFrame':
        ValueError('pandas not implemented yet')
    else:
        raise ValueError(f'Invalid DataFrame type: {type(dataframe)}')


def sql(query: str, dataframe) -> DataFrame:
    pyspark_df: DataFrame = convert_to_pyspark_df(dataframe)
    register_pyspark_df(pyspark_df)
    return sqlContext.sql(query)


dataframe = [{'Name': 'Rigo', 'age': 3}, {'Name': 'Lindsay', 'age': 5}]

sql('select Name, sum(age) from dataframe group by Name', dataframe).show()

#df_empty = pd.DataFrame({'A' : []})
#if type(df_empty):
#    print('test')