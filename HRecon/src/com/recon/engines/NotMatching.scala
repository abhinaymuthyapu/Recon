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

object NotMatching {
  def main (args: Array[String]){
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = org.apache.spark.sql.SparkSession.builder
        .master("local")
        .appName("notMatching")
        .getOrCreate;
   Logger.getLogger("org").setLevel(Level.ERROR)
   val  dff = spark.read.format("csv").option("header", "true").load("src/main/resources/Test1.csv")
   val  dfff = spark.read.format("csv").option("header", "true").load("src/main/resources/Test3.csv")
    dff.createTempView("table1")
    dfff.createTempView("table2")
    val A_B = spark.sql("select * from table1 minus select * from table2").createTempView("A_B")
    val B_A = spark.sql("select * from table2 minus select * from table1").createTempView("B_A")
    val notMathing = spark.sql("select * from A_B union select * from B_A").coalesce(1)
    notMathing.show()  
    notMathing.write.csv("C:\\Users\\718728\\Desktop\\output\\")
    
    def recon(source:DataSet, destination:DataSet){
     val spark = org.apache.spark.sql.SparkSession.builder
        .master("local")
        .appName("notMatching")
        .getOrCreate;
   Logger.getLogger("org").setLevel(Level.ERROR)
   val  dff = spark.read.format("csv").option("header", "true").load("src/main/resources/Test1.csv")
   val  dfff = spark.read.format("csv").option("header", "true").load("src/main/resources/Test3.csv")
    dff.createTempView("table1")
    dfff.createTempView("table2")
    val A_B = spark.sql("select * from table1 minus select * from table2").createTempView("A_B")
    val B_A = spark.sql("select * from table2 minus select * from table1").createTempView("B_A")
    val notMathing = spark.sql("select * from A_B union select * from B_A").coalesce(1)
    notMathing.show()  
    notMathing.write.csv("C:\\Users\\718728\\Desktop\\output\\")
   }
}
}