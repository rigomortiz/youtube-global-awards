package com.datio.example.yga

import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.example.yga.commons.Constants.{JOB_NAME, PARAMETERS}
import com.datio.example.yga.commons.columns.input.VideoInfo
import com.datio.example.yga.commons.columns.output.Trending.{CountryName, Year}
import org.apache.spark.sql.{DataFrame, Dataset}
import com.datio.example.yga.transformations.Transformations.{TrendingDf, VideoInfoDf}
import com.datio.example.yga.utils.IOUtils
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Main entrypoint for YouTubeGlobalAwardsSparkJob process.
  * Implements SparkProcess so it can be found by the Dataproc launcher using JSPI.
  *
  * Configuration for this class should be expressed in HOCON like this:
  *
  * YouTubeGlobalAwardsSparkJob {
  *   ...
  * }
  *
  * This example app reads and writes a csv file contained within the project, for external
  * input or output set the environment variables INPUT_PATH or OUTPUT_PATH, or modify the
  * proper configuration paths.
  *
  */
class YouTubeGlobalAwardsSparkJob extends SparkProcess with IOUtils {

  private val logger = LoggerFactory.getLogger(classOf[YouTubeGlobalAwardsSparkJob])

  override def getProcessId: String = "YouTubeGlobalAwardsSparkJob"

  override def runProcess(context: RuntimeContext): Int = {
    val OK = 0
    val ERR = 1

    Try {
      logger.info(s"Process Id: ${context.getProcessId}")
      val jobConfig = context.getConfig.getConfig("YouTubeGlobalAwardsSparkJob").resolve()
      // Read the input files
      val channelsDf: DataFrame = read(jobConfig.getConfig("input.t_fdev_channels"))
      val videosInfoDf: DataFrame = read(jobConfig.getConfig("input.t_fdev_video_info"))
        .dateFormat()
        .filterVideosLatest()
      val trendingDf = videosInfoDf.join(channelsDf, Seq(VideoInfo.VideoId.name), "inner")
        .addColumns(
          CountryName(),
          Year()
        )

      //Read variables parameters
      val top: Int = jobConfig.getConfig(PARAMETERS).getInt("top")
      val year: Int = jobConfig.getConfig(PARAMETERS).getInt("year")
      val country: String = jobConfig.getConfig(PARAMETERS).getString("country")

      // Get top videos
      val topViewsDf = trendingDf.topViews(top, year, country)
      val topLikesDf = trendingDf.topLikes(top, year, country)
      val topDislikesDf = trendingDf.topDislikes(top, year, country)

      // Union top videos
      val resultDf = topViewsDf.union(topLikesDf).union(topDislikesDf).columnsCast()

      // Write the output files
      write(resultDf, jobConfig.getConfig("output.trending"))
    } match {
      case Success(_) =>
        logger.info("Succesful processing!")
        OK
      case Failure(e) =>
        // You can do wathever exception control you see fit, keep in mind that if the exception
        // bubbles up, it will also be caught at launcher level and the process will return with error
        logger.error("There was an error during the processing of the data", e)
        ERR
    }
  }
}
