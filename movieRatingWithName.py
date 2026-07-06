from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, LongType, StringType
from pyspark.sql.functions import udf

def loadMoviesName() -> dict[int,str]:
    movieNames = {}

    with open('./ml-100k/u.item', 'r', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames

spark = SparkSession.builder.appName("popularMovies").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

nameDict = spark.sparkContext.broadcast(loadMoviesName())

schema =  StructType([
    StructField('userId', IntegerType()),
    StructField('movieId', IntegerType()),
    StructField('rating', IntegerType()),
    StructField('timestamp', LongType())
])

movieRatingDF = spark.read.option('sep', '\t').schema(schema).csv('./ml-100k/u.data')

movieRatingCounts = movieRatingDF.groupBy('movieId').count()

@udf
def lookupNames (movieId: int) -> str:
    return nameDict.value.get(movieId, 'unknown')

movieCountNames = movieRatingCounts.withColumn('movieTitle', lookupNames(F.col('movieId')))
sortedMoviesCountWithName = movieCountNames.orderBy(F.desc('count'))

sortedMoviesCountWithName.show(10, False)



