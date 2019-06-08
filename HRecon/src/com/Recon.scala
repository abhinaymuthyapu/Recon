package com
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._
import com.recon.config.ConfigReader
object Recon {
  def main (args: Array[String]){
   System.setProperty("hadoop.home.dir", "C:\\winutils")
   Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Recon")
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.json("src/resources/people.json")
    df.createTempView("table")
    val df3 = sqlContext.sql("SELECT name FROM table")
   // df3.show()
    val path = "src/resources/people.csv"
    val base_df = sqlContext.read.option("header","true").csv(path)
    base_df.createTempView("table1")
    //val base_df3 = sqlContext.sql("select * from table1 intersect select * from table")
    //base_df3.show()
    val cr = new ConfigReader()
   val _type = cr.getConfig(sc,"config.source.url");
   println(_type)
}
}