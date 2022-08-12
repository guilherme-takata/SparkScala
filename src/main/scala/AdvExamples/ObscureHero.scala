package AdvExamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._
object ObscureHero
{
    case class Heroes(id: Int, numConnections: Int)
    case class Names(id: Int, name: String)

    def main(args: Array[String]): Unit =
        {
            val namesSchema = new StructType().add("id", IntegerType, nullable = true)
              .add("name", StringType, nullable = true)

            Logger.getLogger("org").setLevel(Level.ERROR)
            val spark = SparkSession.builder().master(master = "local[*]").appName("ObscureHero").getOrCreate()

            import spark.implicits._

            val heroLines = spark.sparkContext.textFile("data/Marvel-graph.txt")
            val heroNames = spark.read.option("sep", " ").schema(namesSchema).csv("data/Marvel-names.txt").as[Names]

            val linesParsed = heroLines.map(lineParser)

            val Rdd = linesParsed.reduceByKey((x,y) => x + y)

            val dataset = Rdd.toDS().withColumnRenamed("_1", "id")
              .withColumnRenamed("_2", "NumConnections").as[Heroes]
              .orderBy(col("NumConnections").asc)

            val minConnections = dataset.orderBy(col("NumConnections").asc).first().numConnections

            val filteredDataset = dataset.filter(col("NumConnections") === minConnections)

            val finalDataset = filteredDataset.join(broadcast(heroNames), "id")

            finalDataset.show()
            spark.stop()
        }


    def lineParser(line: String): (Int, Int) =
        {
            val fields = line.split(" ")
            val id = fields(0).toInt
            val numConnections = fields.size - 1

            (id, numConnections)
        }
}
