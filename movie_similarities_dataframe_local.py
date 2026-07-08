import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType


# --------------------------------------------------
# Main Spark setup
# --------------------------------------------------
spark = SparkSession.builder.appName("MovieSimilaritiesDataFrameLocal").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Local file paths
# --------------------------------------------------
RATINGS_PATH = "ml-100k/u.data"
MOVIES_PATH = "ml-100k/u.item"
OUTPUT_PATH = "output/movie-sims-parquet"


# --------------------------------------------------
# Load ratings from u.data
# Format: user_id    movie_id    rating    timestamp
# --------------------------------------------------
ratings_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

ratings = spark.read.option("delimiter", "\t").schema(ratings_schema).csv(RATINGS_PATH).select("user_id", "movie_id", "rating")

print("Ratings data:")
ratings.show(5)


# --------------------------------------------------
# Load movie names from u.item
# Format: movie_id | movie_title | release_date | ...
# --------------------------------------------------
movies_raw = spark.read.option("delimiter", "|").option("encoding", "ISO-8859-1").csv(MOVIES_PATH)

movies = movies_raw.select(
    F.col("_c0").cast("int").alias("movie_id"),
    F.col("_c1").alias("movie_name")
)

print("Movie names:")
movies.show(5, truncate=False)


# --------------------------------------------------
# Create movie rating pairs by joining ratings to itself
# --------------------------------------------------
ratings_a = ratings.alias("a")
ratings_b = ratings.alias("b")

movie_pairs = ratings_a.join(
    ratings_b,
    F.col("a.user_id") == F.col("b.user_id")
).where(
    F.col("a.movie_id") < F.col("b.movie_id")
).select(
    F.col("a.movie_id").alias("movie1"),
    F.col("b.movie_id").alias("movie2"),
    F.col("a.rating").alias("rating1"),
    F.col("b.rating").alias("rating2")
)

print("Movie pairs:")
movie_pairs.show(5)


# --------------------------------------------------
# Compute cosine similarity using DataFrame aggregation
# --------------------------------------------------
pair_scores = movie_pairs.groupBy("movie1", "movie2").agg(
    F.count("*").alias("num_pairs"),
    F.sum(F.col("rating1") * F.col("rating1")).alias("sum_xx"),
    F.sum(F.col("rating2") * F.col("rating2")).alias("sum_yy"),
    F.sum(F.col("rating1") * F.col("rating2")).alias("sum_xy")
)

movie_similarities = pair_scores.withColumn(
    "denominator",
    F.sqrt(F.col("sum_xx")) * F.sqrt(F.col("sum_yy"))
).withColumn(
    "score",
    F.when(F.col("denominator") != 0,
           F.col("sum_xy") / F.col("denominator"))
     .otherwise(0.0)
).select(
    "movie1",
    "movie2",
    "score",
    "num_pairs"
)

print("Movie similarities:")
movie_similarities.show(5)


# --------------------------------------------------
# Save results as Parquet
# --------------------------------------------------
movie_similarities.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"Saved similarity results to Parquet folder: {OUTPUT_PATH}")


# --------------------------------------------------
# Optional: query similar movies from command line
# --------------------------------------------------
if len(sys.argv) > 1:
    movie_id = int(sys.argv[1])

    score_threshold = 0.97
    co_occurrence_threshold = 50

    filtered_results = movie_similarities.where(
        (
            (F.col("movie1") == movie_id) |
            (F.col("movie2") == movie_id)
        ) &
        (F.col("score") > score_threshold) &
        (F.col("num_pairs") > co_occurrence_threshold)
    )

    similar_movies = filtered_results.withColumn(
        "similar_movie_id",
        F.when(F.col("movie1") == movie_id, F.col("movie2"))
         .otherwise(F.col("movie1"))
    ).join(
        movies,
        F.col("similar_movie_id") == F.col("movie_id")
    ).select(
        "movie_name",
        "score",
        "num_pairs"
    ).orderBy(
        F.col("score").desc(),
        F.col("num_pairs").desc()
    )

    original_movie = movies.where(F.col("movie_id") == movie_id).select("movie_name").first()

    if original_movie:
        print(f"\nTop similar movies for: {original_movie['movie_name']}")
        similar_movies.show(10, truncate=False)
    else:
        print(f"Movie ID {movie_id} was not found.")


spark.stop()