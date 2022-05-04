package RDDs

import org.apache.spark._
import org.apache.log4j._

object RDD
{
  def main(args: Array[String]): Unit =
    {
      // Only prints warning if they are errors
      Logger.getLogger("org").setLevel(Level.ERROR)

      // Initiates the SparkContext using all cores of the machine, with the name RDDRatings
      val sc = new SparkContext(master = "local[*]", appName = "RatingsCounter")

      // Load the lines of the file into data of a RDD
      val lines = sc.textFile("data/ml-100k/u.data")

      // Splits each line to a string and splits it out with tabs, taking only the thrid field
      val ratings = lines.map(x => x.split("\t")(2))

      // Counts each time a rating occurs
      val results = ratings.countByValue()

      // Sorts the ratings by their respective counts
      val sortedResults = results.toSeq.sortBy(_._1)

      sortedResults.foreach(println)
    }
}

