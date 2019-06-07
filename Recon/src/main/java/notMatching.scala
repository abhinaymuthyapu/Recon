import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._

object notMatching {
  def main (args: Array[String]){
   System.setProperty("hadoop.home.dir", "C:\\winutils")
   Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "notMatching")
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.json("src/main/resources/Test1.json")
    df.createTempView("table")
    val df3 = sqlContext.sql("SELECT name FROM table")
    val path = "src/main/resources/Test1.csv"
    val base_df = sqlContext.read.option("header","true").csv(path)
    base_df.createTempView("table1")
    val A_B = sqlContext.sql("select * from table1 minus select * from table").createTempView("A_B")
    val B_A = sqlContext.sql("select * from table minus select * from table1").createTempView("B_A")
    val notMathing = sqlContext.sql("select * from A_B full_outer select * from B_A")
    notMathing.show()
}
}