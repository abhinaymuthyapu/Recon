package com.recon.config

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.Row

class ConfigReader(var sc: SparkContext , var cpath: String) {
  
  
  //val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  //val sqlSession = sqlContext.
  //val df = sqlSession.read.option("multiline", "true").json("src/resources/config.json")
    //val df1 = sqlContext.read.json(sc.wholeTextFiles("src/resources/config.json").values)

//val ds: Dataset[Emp] = df.as[Emp]
    //var defcpath = "src/resources/config.json"
    var path = "src/resources/config.json"
    if (cpath != ""){
      path = cpath
    }
    path = "src/resources/config.json"
    def getConfig(path: String): Any = {  
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val df1 = sqlContext.read.json(sc.wholeTextFiles("src/resources/config.json").values)
    return df1.select(path).first().get(0)
    }
    
    def aggParser(row: Row): Aggregation =
    {
      new Aggregation(row.getAs("column"), row.getAs("func"))
    }
    
    def getSource(): DataSet = {
      
    val seq = getConfig("config.source.columns.include").asInstanceOf[WrappedArray[String]]
      val seq1 = getConfig("config.source.columns.exclude").asInstanceOf[WrappedArray[String]]
      val seq2 = getConfig("config.source.key").asInstanceOf[WrappedArray[String]]
      var columns = new Columns(seq.array,seq1.array)
      val aggseq = getConfig("config.source.groupBy").asInstanceOf[WrappedArray[Row]]
      var aggregations = aggseq.map(aggParser).toArray
      var key = seq2.array
      //println(columns)
      //println(aggregations)
      return new DataSet(getConfig("config.source.type").asInstanceOf[String],getConfig("config.source.url").asInstanceOf[String],key,columns,aggregations)
    }
    def getDestination(): DataSet = {
       val seq = getConfig("config.destination.columns.include").asInstanceOf[WrappedArray[String]]
      val seq1 = getConfig("config.destination.columns.exclude").asInstanceOf[WrappedArray[String]]
       val seq2 = getConfig("config.destination.key").asInstanceOf[WrappedArray[String]]
        var key = seq2.array
      var columns = new Columns(seq.array,seq1.array)
      val aggseq = getConfig("config.destination.groupBy").asInstanceOf[WrappedArray[Row]]
      var aggregations = aggseq.map(aggParser).toArray
      //println(columns)
      //println(aggregations)
      return new DataSet(getConfig("config.destination.type").asInstanceOf[String],getConfig("config.destination.url").asInstanceOf[String],key,columns,aggregations)
    }
}