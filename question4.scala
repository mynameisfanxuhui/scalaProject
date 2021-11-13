package Qestion4
// Each library has its significance, I have commented when it's used
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row

object question4 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("question4")
  conf.setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = SparkSession.builder().appName("Question4").config("spark.master", "local").getOrCreate()

  def main (args:Array[String]): Unit = {
    val inputPath = args(0)
    println(inputPath)
    val fileString = sc.textFile(inputPath)
    val fileHeader = "ID,Name,Age,Gender,CountryCode,Salary"

    val schema = StructType(fileHeader.split(",").map(fieldName => StructField(fieldName,StringType, true)))
    val rowRDD = fileString.map(_.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5)))

    val dataFrame = sqlContext.createDataFrame(rowRDD, schema)
    dataFrame.createOrReplaceTempView("Customers")
    val c1 = sqlContext.sql("SELECT avg(Age), Gender " +
      "FROM Customers " +
      "GROUP BY Gender;")
    c1.coalesce(1).write.csv("c1Output")
    val tempC2 = sqlContext.sql("SELECT Count(*) as customerNumber, CountryCode " +
      "FROM Customers " +
      "GROUP BY CountryCode " +
      "ORDER BY COUNT(*);")
    tempC2.createOrReplaceTempView("tempC2")
    //tempC2.coalesce(1).write.csv("tempC2Output")
    val c2 = sqlContext.sql("Select * from tempC2 Limit 2;")
    c2.createOrReplaceTempView("secondResult")
    c2.coalesce(1).write.csv("c2Output")
    val c3 = sqlContext.sql(
      "SELECT Customers.ID,Customers.Name,Customers.Age,Customers.CountryCode,Customers.Salary " +
        "FROM Customers, secondResult " +
        "Where Customers.CountryCode =  secondResult.CountryCode " +
        "and Customers.Gender = 'female';")
    c3.coalesce(1).write.csv("c3Output")
    val c4 = sqlContext.sql(
      "Select C1.CountryCode as xCountryCode, C2.CountryCode as yCountryCode " +
        "from tempC2 C1 " +
        "inner join tempC2 C2 on (C1.customerNumber = C2.customerNumber and C1.CountryCode <> C2.CountryCode);"
    )
    c4.coalesce(1).write.csv("c4Output")
    sc.stop()
  }
}
