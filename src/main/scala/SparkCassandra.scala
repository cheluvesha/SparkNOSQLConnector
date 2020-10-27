import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.spark.sql.SparkSession

object SparkCassandra extends App {
  val spark = SparkSession
    .builder()
    .appName("Demo")
    .master("local[*]")
    .getOrCreate()
  val cassandra_hostname = "localhost"
  val cassandra_port_number = "9042"
  val cassandra_ks_name = "example"
  val cassandra_table_name = "employees"
  val cassandra_cluster_name = "Test Cluster"

  def readDataFromCassandra(): Unit ={
    val user_df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> cassandra_table_name,
        "keyspace" -> cassandra_ks_name,
        "host" -> cassandra_hostname,
        "port" -> cassandra_port_number,
        "cluster" -> cassandra_cluster_name))
      .load()
    user_df.show()
  }
  def writeToCassandra(): Unit = {
    val employeesData = Seq((121,"Sannath","Gulbarga","8569"),(123,"Sharath","mysore","45655"))
    val rddData = spark.sparkContext.parallelize(employeesData)
    rddData.saveToCassandra("example", "employees", SomeColumns("id", "name","address","phone"))
  }
  readDataFromCassandra()
  writeToCassandra()
}
