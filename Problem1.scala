import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;

// this is the code for Problem1
// @author: xli14@wpi.edu


object Problem1 {
  def main(args: Array[String]) {

    // create spark session
    val sparkSession = SparkSession
      .builder()
      .appName("Problem 1")
      .master("local[*]")
      .getOrCreate();

    // create schema for dataframe corresponding to Transactions
    val schema = StructType(
      Array(
        StructField("TransID", IntegerType, nullable = true),
        StructField("CustID", IntegerType, nullable = true),
        StructField("TransTotal", FloatType, nullable = true),
        StructField("TransNumItems", IntegerType, nullable = true),
        StructField("TransDesc", StringType, nullable = true)
      )
    );

    // create dataframe from Transactions.txt
    val dataframe = sparkSession.read
      .option("header", "false")
      .option("delimeter", ",")
      .schema(schema)
      .text("Transactions.txt");

    val T1 = dataframe.where(dataframe("TransTotal") >= 200);

    val T2 = T1.groupBy("TransNumItems").agg(functions.sum("TransTotal"), functions.avg("TransTotal"), functions.min("TransTotal"), functions.max("TransTotal"));
    T2.show();

    val T3 = T1.groupBy("CustID").count();
    T3.show();

    val T4 = dataframe.where(dataframe("TransTotal") >= 600);
    val T5 = T4.groupBy("CustID").count();
    T5.show();

    val Tjoin = T5.join(T3, "CustID");
    val Tfilter = Tjoin.where(T5("countT2")*2<T3("count"));
    val T6 = Tfilter.select("CustID");
    T6.show();

  }
}