package Datasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

/*
Example to find the most popular movie
 */

object MostPopularMovie
{
    case class Movie(MovieId:Int)

    def main(args: Array[String]): Unit =
        {
            Logger.getLogger("org").setLevel(Level.ERROR)

            val spark = SparkSession.builder.appName("MovieRatings").master("local[*]").getOrCreate()
            import spark.implicits._

            val schema = new StructType().add("userId", IntegerType, nullable = false)
              .add("MovieId", IntegerType, nullable = false)
              .add("Rating", IntegerType, nullable = false)
              .add("Timestamp", StringType, nullable = false)

            val dataset = spark.read.option("sep","\t").schema(schema).csv("data/ml-100k/u.data").as[Movie]

            val groupedDs = dataset.groupBy("MovieId").count()

            val ratingsCount = groupedDs.withColumnRenamed("count", "TotalRatings")

            val mostPopMovie = ratingsCount.orderBy(col("TotalRatings").desc).limit(1)

            mostPopMovie.show()

            spark.stop()
        }
}
