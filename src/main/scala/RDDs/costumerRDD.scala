package RDDs

import org.apache.log4j.{Level, Logger}
import org.apache.spark._

object costumerRDD
{
    def main(args: Array[String]): Unit =
      {
        Logger.getLogger("org").setLevel(Level.ERROR)

        //Initialize SparkContext
        val sc = new SparkContext(master = "local[*]", appName = "AmountSpentCustomer")

        val lines = sc.textFile("data/customer-orders.csv")

        val rdd = lines.map(lineParser)

        val totalSpent = rdd.reduceByKey((x, y) => x + y)

        val result = totalSpent.map(x => (x._2, x._1)).sortByKey().collect()

        result.foreach(println)
      }

    def lineParser(line: String): (Int, Float) =
      {
        val fields = line.split(",")
        val customerId = fields(0).toInt
        val amountSpent = fields(2).toFloat

        (customerId, amountSpent)
      }
}
