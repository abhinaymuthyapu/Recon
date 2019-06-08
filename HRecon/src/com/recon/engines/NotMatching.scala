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

class NotMatching(var sc: SparkContext) {
  
  def recon(source:DataSet, destination:DataSet){
    
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
     val spark = org.apache.spark.sql.SparkSession.builder
        .master("local")
        .appName("notMatching")
        .getOrCreate;
   Logger.getLogger("org").setLevel(Level.ERROR)
   val  dff = sqlContext.read.json(source.url)
   val  dfff = spark.read.format("csv").option("header", "true").load(destination.url)
    dff.createTempView("table31")
    dfff.createTempView("table32")
    var srccol = source.columns.include.mkString(",");
    var destcol = destination.columns.include.mkString(",");
    //println(srccol)
    //println(destcol)
    val A_B = spark.sql("select "+srccol+ " from table31 minus select "+destcol+" from table32").createTempView("A_B")
    val B_A = spark.sql("select "+destcol+" from table32 minus select "+srccol+" from table31").createTempView("B_A")
    val notMathing = spark.sql("select "+srccol+" from A_B union select "+destcol+" from B_A").coalesce(1)
    notMathing.show()  
    notMathing.write.csv("C:\\Users\\718728\\Desktop\\notmatchingoutput\\")
   }
}