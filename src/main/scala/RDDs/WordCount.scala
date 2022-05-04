package RDDs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc: SparkContext = new SparkContext(master = "local[*]", appName = "WordCount")
    val lines = sc.textFile(path = "data/book.txt")

    val linesParsed = lines.flatMap(lineParserBetter)

    // Mapping each word to a tuple, containing the word observed and the count of it
    val wordsCounting = linesParsed.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    wordsCounting.map(x => (x._2, x._1)).sortByKey(ascending = false).foreach(println)
  }

  def lineParser(line: String): Array[String] =
  /*
    This function returns the counting of each word, but it has a problem with punctuation and capital letters, the other implementation deals with those problems
    */ {
    return line.split(" ")
  }

  def lineParserBetter(line: String): Array[String] = {
    line.toLowerCase().split("\\W+")
  }
}
