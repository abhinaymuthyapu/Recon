package com.recon.config

class Config(var source: DataSet, var destination: DataSet) {}

class DataSet(var _type : String, var url: String, var key: Array[String], var columns: Columns, var groupBy: Array[Aggregation]) {}

class Columns(var include: Array[String], var exclude: Array[String]){}

class Aggregation(var column: String, var func: String){}
