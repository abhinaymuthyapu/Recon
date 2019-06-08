package com.recon.config

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.Row

class ConfigReader(var sc: SparkContext ) {
  
  
  //val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  //val sqlSession = sqlContext.
  //val df = sqlSession.read.option("multiline", "true").json("src/resources/config.json")
    //val df1 = sqlContext.read.json(sc.wholeTextFiles("src/resources/config.json").values)

//val ds: Dataset[Emp] = df.as[Emp]
      
    def getConfig(path: String): Any = {  
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val df1 = sqlContext.read.json(sc.wholeTextFiles("src/resources/config.json").values)
    return df1.select(path).first().get(0)
    }
    def getSource(): DataSet = {
    //def getSource() {  
      
      def aggParser(row: Row): Aggregation =
    {
      new Aggregation(row.getAs("column"), row.getAs("func"))
    }
    val seq = getConfig("config.source.columns.include").asInstanceOf[WrappedArray[String]]
      val seq1 = getConfig("config.source.columns.exclude").asInstanceOf[WrappedArray[String]]
      var columns = new Columns(seq.array,seq1.array)
      val aggseq = getConfig("config.source.groupBy").asInstanceOf[WrappedArray[Row]]
      var aggregations = aggseq.map(aggParser).toArray
      println(columns)
      println(aggregations)
      return new DataSet(getConfig("config.source.type").asInstanceOf[String],getConfig("config.source.url").asInstanceOf[String],columns,aggregations)
    }
    /*def getDestination(): DataSet = {
      return 0
    }*/
}