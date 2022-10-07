package com.datio.example.yga.steps

import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.example.yga.utils.Common
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers

class Given extends ScalaDsl with EN with Matchers {
  lazy val datioSpark: DatioSparkSession = DatioSparkSession.getOrCreate()
  Given("""^a config file with path: (.*)$""") {
    path: String => {
      Common.configPath = path
    }
  }

  Given("""^a dataframe (\S+) in path: (.*)$""") {
    (dfName: String, path: String) => {
      val df: DataFrame = datioSpark.read().parquet(path)
      Common.dfMap.put(dfName, df)
    }
  }
}
