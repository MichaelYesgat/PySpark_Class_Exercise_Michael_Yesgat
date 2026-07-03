from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, udtf
from pyspark.sql.types import StringType
import re


spark = SparkSession.builder.appName('HashtagExtractor').getOrCreate()
spark.sparkContext.setLogLevel('WARN')


data = [
    ("Learning #AI with #ML",),
    ("Explore #DataScience",),
    ("Not a hashtag in sight",)
]

df = spark.createDataFrame(data, ['text'])


@udf(returnType=StringType())
def count_hashtags(text: str):
    if text:
        return len(re.findall(r"#\w+", text))


@udtf(returnType="hashtag: string")
class HashtagExtractor:
    def eval(self, text: str):
        if text:
            hashtags = re.findall(r"#\w+", text)
            for hashtag in hashtags:
                yield (hashtag,)


spark.udf.register('count_hashtags', count_hashtags)
spark.udtf.register('HashtagExtractor', HashtagExtractor)


spark.sql("SELECT count_hashtags('Welcome to #ApacheSpark and #BigData') AS hashtag_count").show()

df.selectExpr('text', 'count_hashtags(text) AS num_hashtags').show()

spark.sql("SELECT * FROM HashtagExtractor('Welcome to #ApacheSpark and #BigData')").show()

df.createOrReplaceTempView('tweets')

spark.sql("SELECT text, hashtag FROM tweets, LATERAL HashtagExtractor(text)").show()


spark.stop()