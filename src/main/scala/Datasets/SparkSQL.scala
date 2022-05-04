package Datasets

import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQL
{
    case class Person(id: Int, name: String, age: Int, friends: Int)

    def main(args: Array[String]): Unit =
        {
            Logger.getLogger("org").setLevel(Level.ERROR)

            val spark = SparkSession.builder().appName("SparkSQL").master("local[*]").getOrCreate()

            import spark.implicits._

            // The as[Person] method transform the dataframe into a dataset with a declared schema, through the use of the Person object
            val schemaPeople = spark.read.option("header", "true").option("inferSchema", "true").csv("data/fakefriends.csv").as[Person]

            schemaPeople.printSchema()

            //Creates a view of the dataset
            schemaPeople.createOrReplaceTempView("people")

            //Once the view is created you can query it by using common SQL commands
            val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

            teenagers.show()

            // Make sure to always stop the session
            spark.stop()
        }
}
