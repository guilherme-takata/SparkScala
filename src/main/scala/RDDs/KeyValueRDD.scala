package RDDs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object KeyValueRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initiates the SparkContext
    val sc = new SparkContext(master = "local[*]", appName = "FriendsAge")

    // Read the csv(text) file into an rdd
    val lines = sc.textFile("data/fakefriends-noheader.csv")
    val rdd = lines.map(parseLines)

    // Through mapping values we keep a count of each occurrence based on age, then we sum the number of observation and number of friends
    val reducedRdd = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // Calculates the averages and collects(computes) the final result
    val result = reducedRdd.mapValues(x => x._1 / x._2).collect()

    // Prints the results
    result.sorted.foreach(println)
  }

  def parseLines(line: String): (Int, Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt

    (age, numFriends)
  }
}
