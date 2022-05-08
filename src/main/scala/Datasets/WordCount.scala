package Datasets
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark._

object WordCount
{
  case class Book(value: String)

  def main(args: Array[String]): Unit =

    {
      val spark = SparkSession.builder().master("local[*]").appName("WordCountExample").getOrCreate()

      val rddDataset = RddDatasets(spark)

      val dataset = Dataset(spark)

      rddDataset.show()

      dataset.show()

      spark.stop()
    }

  def Dataset(spark: SparkSession): Dataset[Row] =
  {
    /*
    Example of word counting, the same as in the RDD case but this time using the dataset API
    Since we are dealing with an unstructured data file, in this case an RDD would be more straightforward and require a lot less pre-processing
    So loading it as an RDD and later converting it to a DataSet would be the best approach in this situation
    */

    Logger.getLogger("org").setLevel(Level.ERROR)

    import spark.implicits._

    val dataset = spark.read.text("data/book.txt").as[Book]

    //Splitting the lines into individual words
    val wordsDataset = dataset
      .select(explode(split($"value", "\\W+")) // Explodes the lines of the file into individual words
      .alias("Word")) // Renames the column from values to Word
      .filter($"Word" =!= "") // Filters for rows with non empty strings

    val lowerCaseDataset = wordsDataset.select(lower($"Word").alias("LowerCaseWords"))

    val wordCounts = lowerCaseDataset.groupBy("LowerCaseWords").count()

    val wordCountsSorted = wordCounts.sort("count") // Sorting the result by their number of occurrences

    wordCountsSorted
  }

  def RddDatasets(spark: SparkSession): Dataset[Row] =
    {
      /*
      The same example but this time using a mix of RDDs and Datasets, the usage of RDDs makes the pre-processing much simpler
       */

      import spark.implicits._

      Logger.getLogger("org").setLevel(Level.ERROR)

      val rdd = spark.sparkContext.textFile("data/book.txt")

      val linesParsed = rdd.flatMap(x => x.split("\\W+"))

      val dataset = linesParsed.toDS()

      val lowerWords = dataset.select(lower($"value").alias("word"))

      val wordsCount = lowerWords.groupBy("word").count()

      val wordsCountSorted = wordsCount.sort("count")

      wordsCountSorted
    }
}
