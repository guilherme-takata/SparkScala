package Datasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object DataframeDatasets
{
  case class Person(id: Int, name: String, age: Int, friends: Int)

  // Same example of the Person dataset but performing operations using the dataset API instead of SQL commands
  def main(args: Array[String]): Unit =
    {
      Logger.getLogger("org").setLevel(Level.ERROR)

      val sparkSession = SparkSession.builder().appName("DatabaseExample").master("local[*]").getOrCreate()

      import sparkSession.implicits._

      val dataset = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("data/fakefriends.csv").as[Person]

      //Now we can do some operations like in the SQL example but by using the dataset API, without having to make temp views

      dataset.filter(dataset("age") < 21).show()

      dataset.groupBy("age").count().show()

      sparkSession.stop()

    }
}
