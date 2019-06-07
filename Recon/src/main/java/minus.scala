import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._

object minus {
   def main (args: Array[String]){
   System.setProperty("hadoop.home.dir", "C:\\winutils")
   try{
     Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "ReconRR")
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.json("src/main/resources/people.json")
    df.createTempView("table")
    val df3 = sqlContext.sql("SELECT name FROM table")
    val path = "src/main/resources/people.csv"
    val base_df = sqlContext.read.option("header","true").csv(path)
    base_df.createTempView("table1")
     val base_df3 = sqlContext.sql("select * from table1 minus select * from table")
    base_df3.show()
   }
  catch{
     case ex: Exception =>{
     ex.printStackTrace
   }
  }
}

}