package com.recon.engines
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._
import org.apache.spark.sql.types._
import java.io.PrintWriter
import java.io._
import scala.io.Source
import com.recon.config._
import com.mysql.jdbc.Driver._

object MHive {
  def main (args: Array[String]){
  System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = org.apache.spark.sql.SparkSession.builder
        .master("local")
        .appName("notMatching")
        .getOrCreate;
   Logger.getLogger("org").setLevel(Level.ERROR)
   val dataframe_mysql = spark.read.format("jdbc")
   .option("url", "jdbc:mysql://sql12.freemysqlhosting.net")
   .option("driver", "com.mysql.jdbc.Driver")
   .option("dbtable", "CONGIG_TB")
   .option("user", "sql12294874").option("password", "JdfKX6BAef").load()
  dataframe_mysql.show
}
}