package com.recon.config

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._


class ConfigReader {
  
    def getConfig(sc: SparkContext, path: String): Any = {  
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val df1 = sqlContext.read.json(sc.wholeTextFiles("src/resources/config.json").values)
    return df1.select(path).first().get(0)
    }
}