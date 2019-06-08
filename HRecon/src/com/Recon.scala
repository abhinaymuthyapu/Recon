package com
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._
import com.recon.config.ConfigReader
import com.recon.engines.NotMatching
import com.recon.engines.common
import com.recon.engines.LeftOuter
import com.recon.engines._

object Recon {
  def main (args: Array[String]){
   System.setProperty("hadoop.home.dir", "C:\\winutils")
   var cpath = ""
  // if(args.isEmpty){
    
   //}else{
     //cpath = args(0)}
   Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Recon")
//    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
//    val df = sqlContext.read.json("src/resources/people.json")
//    df.createTempView("table")
//    val df3 = sqlContext.sql("SELECT name FROM table")
   // df3.show()
   // val path = "src/resources/people.csv"
    //val base_df = sqlContext.read.option("header","true").csv(path)
    //base_df.createTempView("table1")
    //val base_df3 = sqlContext.sql("select * from table1 intersect select * from table")
    //base_df3.show()
    val cr = new ConfigReader(sc,cpath)
    val lo = new LeftOuter(sc)
   //val _type = cr.getConfig("config.source.url");
   //println(_type)
   val src = cr.getSource()
   val dest = cr.getDestination()
   //println(s._type)
   
   
   //<!-- Calling starts Here ---!> 
  val r = new NotMatching(sc)
  r.recon(src,dest)
//   
  val r1 = new common(sc)
   r1.recon(src,dest)
   
   
 val r3 = new Minus(sc)
        r3.recon(src,dest) 
 
        val r2 = new LeftOuter(sc)
  r2.recon(src,dest)

       
   
   
}
}