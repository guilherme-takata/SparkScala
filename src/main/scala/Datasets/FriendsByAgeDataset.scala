package Datasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark._

object FriendsByAgeDataset
{
  //The same example of friends by age, but this time using the dataset and dataframe APIs
  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(arg: Array[String]): Unit =
    {
      Logger.getLogger("org").setLevel(Level.ERROR)

      val spark = SparkSession.builder().appName("FakeFriendsRevisited").master("local[*]").getOrCreate()

      import spark.implicits._

      val dataset = spark.read.option("header", "true").option("inferSchema", "true").csv("data/fakefriends.csv").as[Person]

      // This is the more straightforward way of doing things
      val groupedData1 = dataset.groupBy("age").avg("friends").as("AvgFriends")

      // This one uses the agg method, and applies two functions on the data
      val groupedData2 = dataset.groupBy("age").agg(sum("friends").as("NumFriends"), count(lit(1)).as("Number of occurences"))

      val groupedData2Result = groupedData2.withColumn("AvgFriends", groupedData2("NumFriends") / groupedData2("Number of occurences"))

      groupedData1.show()

      groupedData2Result.select("Age", "AvgFriends").show()

      spark.stop()

    }

}
