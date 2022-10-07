package com.datio.example.yga.utils

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object Common {
  var configPath: String = _
  var exitCode: Int = _

  val dfMap: mutable.Map[String, DataFrame] = mutable.Map()
}
