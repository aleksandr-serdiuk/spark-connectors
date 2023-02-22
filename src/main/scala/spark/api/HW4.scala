package spark.api
import org.apache.spark.sql.Row
import java.util.Properties

object HW4 extends App {
  import org.apache.spark.sql.{DataFrame, SparkSession}

    val pg_url = "jdbc:postgresql://localhost:5432/spark"
    val pg_username = "docker"
    val pg_pwd = "docker"

    val spark = SparkSession
      .builder()
      .appName("JDBC to DF")
      .config("spark.master", "local[4]")
      .getOrCreate()

    val pg_table = "employees"
    val partition_size = 5000

    val from_custom_DF = spark.read
      .format("org.example.datasource.postgres_hw")
      .option("url", pg_url)
      .option("user", pg_username)
      .option("password", pg_pwd)
      .option("tableName", pg_table)
      .option("partitionSize", partition_size)
      .load()

  //from_custom_DF.schema.fields

  def printPartitionsNumber(inDF: DataFrame) = println(s"Partition number =  ${inDF.rdd.getNumPartitions}")


  from_custom_DF.show()
  println(s"Rows count =  ${from_custom_DF.count()}")
  printPartitionsNumber(from_custom_DF)

}
