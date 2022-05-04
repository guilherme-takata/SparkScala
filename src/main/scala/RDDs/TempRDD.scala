package RDDs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import java.text.SimpleDateFormat
import java.util.Date

object TempRDD {
  /*
    Script to find the minimum temperature by the station id, using key/value RDDs and the filter method
  */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext(master = "local[*]", appName = "RDDFilter")
    val lines = sc.textFile(path = "data/1800.csv")

    val parsedLines = lines.map(parseLines)

    // Filters the rows for the ones that only have the minimum temperature, similarly you can change the string to get the maximum temps
    val rdd = parsedLines.filter(x => x._3 == "TMIN")

    val filteredRDD = rdd.map(x => (x._1, x._4))

    // Reduces to the minimum observation.
    val reducedRdd = filteredRDD.reduceByKey((x, y) => Math.min(x, y))

    reducedRdd.collect().foreach(println)
  }

  def parseLines(line: String): (String, Date, String, Float) =
  //Method to parse lines and return only the relevant fields back to the RDD in the form of a tuple
  {
    val fields = line.split(",")
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val date = dateFormat.parse(fields(1))

    val stationId = fields(0)
    val tempDescription = fields(2)
    val temp = fields(3).toInt * 0.1

    (stationId, date, tempDescription, temp.toFloat)
  }
}
