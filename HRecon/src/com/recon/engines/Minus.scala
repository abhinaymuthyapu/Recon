package com.recon.engines

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._
import com.recon.config._

class Minus (var sc: SparkContext ) {
   def recon(source:DataSet, destination:DataSet){
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  try{
     Logger.getLogger("org").setLevel(Level.ERROR)
    //val sc = new SparkContext("local[*]", "ReconRR")
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.json(source.url)
    df.createTempView("table4")
    //val df3 = sqlContext.sql("SELECT name FROM table")
    val path = destination.url
    val base_df = sqlContext.read.option("header","true").csv(path)
    base_df.createTempView("table41")
    var srccol = source.columns.include.mkString(",");
    var destcol = destination.columns.include.mkString(",");
     val base_df3 = sqlContext.sql("select "+srccol+" from table41 minus select "+destcol+" from table4")
    base_df3.show()
    base_df3.write.csv("C:\\Users\\718728\\Desktop\\minusoutput\\")
   }
  catch{
     case ex: Exception =>{
     ex.printStackTrace
   }
  }

   }  
}