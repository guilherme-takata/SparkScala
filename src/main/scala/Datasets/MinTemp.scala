package Datasets
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MinTemp
{
  case class Temperature(StationId: String, Date: Int, Order: String, Measurement: Int)

  def main(args: Array[String]): Unit =
    {
      Logger.getLogger("org").setLevel(Level.ERROR)

      val sparkSession = SparkSession.builder.appName("MinTemp").master("local[*]").getOrCreate()
      import sparkSession.implicits._


      /*Since in this case there's no header we need to construct and declare the schema explicitly to the dataset,
      the following lines declare this explicitly and gives the types and column names*/
      val tempSchema = new StructType()
        .add("StationId", StringType, nullable = true)
        .add("Date", IntegerType, nullable = true)
        .add("Order", StringType, nullable = true)
        .add("Measurement", IntegerType, nullable = true)

      val dataset = sparkSession.read.schema(tempSchema).csv("data/1800.csv").as[Temperature]

      /*
      dataset.show() As you can see the dataset now has the correct column names and schema
      dataset.printSchema()
      */

      val minTempDs = dataset.filter($"Order" === "TMIN")

      val minTempStation = minTempDs.groupBy("StationId").min("Measurement")

      val result = minTempStation.withColumn("MinTemp", $"min(Measurement)" / 10)

      result.show()

      sparkSession.stop()
    }
}
