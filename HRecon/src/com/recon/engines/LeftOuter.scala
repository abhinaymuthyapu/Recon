package com.recon.engines

import com.recon.config._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._
import com.recon._


class LeftOuter(var sc: SparkContext ) {
  def recon(source:DataSet, destination:DataSet){
     //System.setProperty("hadoop.home.dir", "C:\\winutils")
     Logger.getLogger("org").setLevel(Level.ERROR)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.json(source.url)
    df.createTempView("table")
    val df3 = sqlContext.sql("SELECT * FROM table")
    val path = destination.url
    val base_df = sqlContext.read.option("header","true").csv(path)
    base_df.createTempView("table1")
    val  columnsdf1 = source.key
    val columnsdf2 = destination.key
    
    val joinExprs = columnsdf1
     .zip(columnsdf2)
     .map{case (c1, c2) => df(c1) === base_df(c2)}
     .reduce(_ && _)    
    println(joinExprs)
    
    val leftouter_join = df.join(base_df, joinExprs, "left_outer")
    leftouter_join.show()  
    //leftouter_join.write.csv("C:\\Users\\718728\\Desktop\\leftOuteroutput\\")
  }
  
}