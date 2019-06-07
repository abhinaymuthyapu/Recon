import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._

object leftOuter {
   def main (args: Array[String]){
     System.setProperty("hadoop.home.dir", "C:\\winutils")
  Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "ReconRR")
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.json("src/main/resources/people.json")
    df.createTempView("table")
    val df3 = sqlContext.sql("SELECT name FROM table")
    val path = "src/main/resources/people.csv"
    val base_df = sqlContext.read.option("header","true").csv(path)
    base_df.createTempView("table1")
    val outer_join = df.join(base_df, df("name") === base_df("name"), "left_outer")
    outer_join.show()
   }
}