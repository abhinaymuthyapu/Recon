package com.recon.engines

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._
import com.recon.config._

class common(var sc: SparkContext )  {
  def recon(source:DataSet, destination:DataSet){
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    try{
     Logger.getLogger("org").setLevel(Level.ERROR)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.json(source.url)
    df.createTempView("table2")
    //val df3 = sqlContext.sql("SELECT name FROM table")
    val path = destination.url
    val base_df = sqlContext.read.option("header","true").csv(path)
    base_df.createTempView("table21")
    df.join(base_df, "name").show
    df.write.csv("C:\\Users\\718728\\Desktop\\commonoutput\\")
   }
  catch{
     case ex: Exception =>{
     ex.printStackTrace
   }
  }
    
  }
}