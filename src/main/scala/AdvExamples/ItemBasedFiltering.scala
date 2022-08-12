package AdvExamples

import AdvExamples.MovieIdNames.movieNames
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

object ItemBasedFiltering
{
    case class Movie(userId: Int, movieId: Int, rating: Int)
    case class MoviePairs(Movie1: Int, Movie2: Int, Rating1: Int, Rating2: Int)
    case class Similarities(Movie1: Int, Movie2: Int, Similarity: Float, NumPairs: Int)
    case class MovieNames(MovieId: Int, MovieName: String)

    def cosineSimilarity(spark: SparkSession, ds: Dataset[MoviePairs]): Dataset[Similarities] =
        {
            //Compute the vectors for the similarity computation
            val scores = ds.withColumn("xx",col("Rating1") * col("Rating1"))
              .withColumn("xy", col("Rating1") * col("Rating2"))
              .withColumn("yy", col("Rating2") * col("Rating2"))

            //Set numerators and denominators for the cosine similarity function
            val similarities = scores.groupBy("Movie1", "Movie2")
              .agg(sum("xy").alias("numerator"), (sqrt(sum("xx")) * sqrt(sum("yy"))).alias("denominator"),
                  count("xy").alias("NumPairs"))
              .withColumn("Similarity", when(col("denominator") =!= 0, col("numerator") / col("denominator")).otherwise(null))

            import spark.implicits._

            val result = similarities.select("Movie1", "Movie2", "Similarity", "NumPairs").as[Similarities]

            result;
        }

    def main(args: Array[String]): Unit =
        {
            Logger.getLogger("org").setLevel(Level.ERROR)
            val spark = SparkSession.builder().appName("CollaborativeFiltering").master("local[*]").getOrCreate()

            val nameMap = spark.sparkContext.broadcast(movieNames())

            import spark.implicits._

            val movieSchema = new StructType()
              .add("UserId", IntegerType, nullable = true)
              .add("MovieId", IntegerType, nullable = true)
              .add("Rating", IntegerType, nullable = true)

            val movies = spark.read.schema(movieSchema).option("sep", "\t").csv("data/ml-100k/u.data").as[Movie]

            movies.show()

            val moviePairs = movies.as("movies1")
              .join(movies.as("movies2"), $"movies1.UserId" === $"movies2.UserId" && $"movies1.MovieId" < $"movies2.MovieId" )
              .select($"movies1.MovieId".alias("Movie1")
                  ,$"movies2.MovieId".alias("Movie2")
                  ,$"movies1.Rating".alias("Rating1")
                  ,$"movies2.Rating".alias("Rating2")
                  ).as[MoviePairs]

            val similarities = cosineSimilarity(spark, moviePairs).cache() //Store the dataset into memory for easy access and constant use

            if (args.length > 0)
            {
                val scoreThresholds = 0.97
                val minOcurrences = 50.0

                val movieId = args(0).toInt

                // Filter the dataset based on our
                val resultsFiltered = similarities.filter(($"Movie1" === movieId || $"Movie2" === movieId) && ($"Similarity" >= scoreThresholds) && $"NumPairs" >= minOcurrences)

                val sortedResults = resultsFiltered.sort(col("Similarity").desc).take(10)

                for (result <- sortedResults){

                    var similarMovieId = result.
                    if

                }
            }


            spark.stop()
        }
}
