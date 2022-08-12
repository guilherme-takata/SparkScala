package Datasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
Revisiting the customer spent amount example but this time using the Dataset API
*/

object CustomerSpentValue
{
    case class Customer(customerId: Int, productId: Int, price: Float)

    def main(args: Array[String]): Unit =
        {
            Logger.getLogger("org").setLevel(Level.ERROR)

            val spark = SparkSession.builder().appName("Customers").master("local[*]").getOrCreate()

            import spark.implicits._

            val schema = new StructType().add("customerId", IntegerType, nullable = false) // Creating the schema for the dataset, since the file has no header
                        .add("productId", IntegerType, nullable = false)
                        .add("price", FloatType, nullable = false)

            val dataset = spark.read.schema(schema).csv("data/customer-orders.csv").as[Customer]
            val groupedDataset = dataset.groupBy("customerId").agg(sum("price").alias("TotalSpent"))

            val result = groupedDataset.withColumn("TotalSpentRounded", round(col("TotalSpent"), 2))

            result.select("customerId", "TotalSpentRounded").show()

            spark.stop()
        }
}
