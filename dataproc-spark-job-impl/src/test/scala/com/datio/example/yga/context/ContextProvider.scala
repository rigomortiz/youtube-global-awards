package com.datio.example.yga.context

import com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Suite}

trait ContextProvider extends FlatSpec with BeforeAndAfterAll with Matchers {
  self: Suite =>
  @transient
  var spark: SparkSession = _
  val config: Config = new ProcessConfigLoader().fromPath("src/test/resources/config/application-test.conf")

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession.builder()
      .appName("sparkSession")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    super.afterAll()

    if (spark != null) {
      spark.stop()
    }
  }

}
