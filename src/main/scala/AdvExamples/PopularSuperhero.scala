package AdvExamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._
/*
Find the most popular superhero, the one with most connections
*/

object PopularSuperhero
{
    case class MarvelHeroes(value: String)
    case class HeroesNames(id: Int, name: String)

    def main(args: Array[String]): Unit =
        {
            Logger.getLogger("org").setLevel(Level.ERROR)
            val spark = SparkSession.builder().appName("Superheroes").master("local[*]").getOrCreate()
            import spark.implicits._

            val namesSchema = new StructType().add("id", IntegerType, nullable = true)
              .add("name", StringType, nullable = true)

            val namesLines = spark.read.option("sep", " ").schema(namesSchema).csv("data/Marvel-names.txt").as[HeroesNames]

            val heroesLines = spark.read.text("data/Marvel-graph.txt").as[MarvelHeroes]

            val heroes = heroesLines.withColumn("ID", split(col("value"), " ")(0))
              .withColumn("NumConnections", size(split(col("value"), " ")) - 1)
              .groupBy("id").agg(sum("NumConnections").alias("TotalConnections"))
              .sort($"TotalConnections".desc)

            val popularId = heroes.select("id").first().get(0)


            spark.stop()
        }
}
