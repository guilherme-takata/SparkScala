package AdvExamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}

/*
Example of using broadcast variables to map each movie Id to its respective name
 */
object MovieIdNames {
    case class Movie(userId: Int, movieId: Int, rating: Int, timestamp: Long)

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession.builder().master("local[*]").appName("MovieNames").getOrCreate()
        import spark.implicits._

        val nameMap = spark.sparkContext.broadcast(movieNames()) //Sends (broadcasts) the map to all the spark executors

        val movieSchema = new StructType()
          .add("UserId", IntegerType, nullable = true)
          .add("MovieId", IntegerType, nullable = true)
          .add("Rating", IntegerType, nullable = true)
          .add("Timestamp", LongType, nullable = true)

        val dataset = spark.read.option("sep", "\t").schema(movieSchema).csv("data/ml-100k/u.data").as[Movie]

        val movieCounts = dataset.groupBy("MovieId").count()

        val lookupName = (movieId: Int) => nameMap.value(movieId) // Declaring a lambda function to lookup the name of the movie from its ID

        val lookupNameUdf = udf(lookupName)

        val movieDSNamed = movieCounts.withColumn("MovieName", lookupNameUdf(col("MovieId")))

        val result = movieDSNamed.sort(col("count").desc)

        result.show()

        spark.stop()
    }

    def movieNames(): Map[Int, String] = {
        implicit val codec: Codec = Codec("ISO-8859-1")

        val file = Source.fromFile("data/ml-100k/u.item")

        val lines = file.getLines()

        val movieNames = lines.map(x => x.split('|')).map(arr => arr(0).toInt -> arr(1)).toMap

        file.close()

        movieNames
    }


}
