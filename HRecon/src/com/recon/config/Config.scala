package com.recon.config

class Config(var source: DataSet, var destination: DataSet) {}

class DataSet(var _type : String, var url: String, var columns: Columns, var groupBy: Array[Aggregation]) {}

class Columns(var include: String, var exclude: String){}

class Aggregation(var column: String, var func: String){}