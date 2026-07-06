from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName('SuperHeroes').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType())
])

names = spark.read.schema(schema).option('sep', ' ').csv('./MarvelNames.txt')

lines = spark.read.text('MarvelGraph.txt')

connections = lines.withColumn('id', F.split(F.col('value'), ' ')[0]) \
    .withColumn('connections', F.size(F.split(F.col('value'), ' ')) - 1) \
    .groupBy('id').agg(F.sum('connections').alias('connections'))

mostPopular = connections.sort(F.col('connections').desc()).first()
mostPopularName = names.filter(F.col('id') == mostPopular[0]).select('name').first()

print(mostPopularName[0] + ' is the most popular superhero with ' + str(mostPopular[1]) + ' appearences.')


# -------------------------------------------------------
# Challenge: Find the most obscure heroes
# -------------------------------------------------------

minConnections = connections.agg(F.min('connections')).first()[0]
obscureHeroes = connections.filter(F.col('connections') == minConnections)
obscureHeroesWithNames = obscureHeroes.join(names,'id').select('name','connections').sort('name')

print('The most obscure heroes are:')

obscureHeroesWithNames.show(obscureHeroesWithNames.count(), False)

spark.stop()