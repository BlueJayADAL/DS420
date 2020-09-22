import datetime
from pyspark.sql import SparkSession

# load mongo data
input_uri = 'mongodb://127.0.0.1/movielens.users'
output_uri = 'mongodb://127.0.0.1/movielens.users'

my_spark = SparkSession\
    .builder\
    .appName("MongodbSparkConnectorDemo")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:3.0.0')\
    .getOrCreate()

df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()

print(df.show())