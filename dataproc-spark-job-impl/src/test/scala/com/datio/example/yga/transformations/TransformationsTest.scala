package com.datio.example.yga.transformations

import com.datio.example.yga.commons.Constants.{INT, ROW_NUMBER}
import com.datio.example.yga.commons.columns.input.VideoInfo
import com.datio.example.yga.commons.columns.output.Trending
import com.datio.example.yga.commons.columns.output.Trending.{CountryName, Year}
import com.datio.example.yga.context.ContextProvider
import com.datio.example.yga.utils.IOUtils
import org.apache.spark.sql.DataFrame
import com.datio.example.yga.transformations.Transformations._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class TransformationsTest extends ContextProvider with IOUtils {
  val top = 10
  val year = 2018
  val country = "MX"

  "dateFormat method" should "return a DF in column 'trending_date' with format yyyy-MM-dd" in {
    val path = config.getConfig("YouTubeGlobalAwardsSparkJob.input.t_fdev_video_info")
    val dataFrame: DataFrame = read(path)
    val output = dataFrame.dateFormat()
    val p = "[0-9]{4}-[0-9]{2}-[0-9]{2}"

    output.withColumn("pattern", regexp_replace(VideoInfo.TrendingDate.column, p, ""))
      .filter(col("pattern") =!= "").count() shouldBe 0
  }

  "filterVideosLatest method" should "return a DF with the latest trending date" in {
    val path = config.getConfig("YouTubeGlobalAwardsSparkJob.input.t_fdev_video_info")
    val dataFrame: DataFrame = read(path)
    val output = dataFrame.filterVideosLatest()

    val rowNumberCol = col(ROW_NUMBER)
    val windowSpec = Window.partitionBy(VideoInfo.VideoId.column).orderBy(VideoInfo.TrendingDate.column.desc)
    val videosPrevious = dataFrame.distinct()
      .exceptAll(output)
      .select(dataFrame.columns.map(s => col(s)) :+ row_number().over(windowSpec).alias(ROW_NUMBER) :_*)
      .filter(rowNumberCol === 1)
      .select(VideoInfo.VideoId.column, VideoInfo.TrendingDate.column.alias("trending_date_previous"))


    output.join(videosPrevious, Seq(VideoInfo.VideoId.name), "inner")
      .select(VideoInfo.TrendingDate.column, VideoInfo.VideoId.column, col("trending_date_previous"))
      .filter(VideoInfo.TrendingDate.column < col("trending_date_previous"))
      .count() shouldBe 0
  }

  "topViews method" should "return a DataFrame with the list of videos top" in {
    val channelsDf: DataFrame = read(config.getConfig("YouTubeGlobalAwardsSparkJob.input.t_fdev_channels"))
    val videosInfoDf: DataFrame = read(config.getConfig("YouTubeGlobalAwardsSparkJob.input.t_fdev_video_info"))
      .dateFormat()
      .filterVideosLatest()
    val output = videosInfoDf.join(channelsDf, Seq(VideoInfo.VideoId.name), "inner")
      .addColumns(
        CountryName(),
        Year()
      ).topViews(top, year, country)

    output.filter(Trending.Year.column =!= year && Trending.Country.column =!= country &&
      Trending.CategoryEvent.column =!= "1").count() shouldBe 0
  }

  "topLikes method" should "return a DataFrame with the list of videos top likes" in {
    val channelsDf: DataFrame = read(config.getConfig("YouTubeGlobalAwardsSparkJob.input.t_fdev_channels"))
    val videosInfoDf: DataFrame = read(config.getConfig("YouTubeGlobalAwardsSparkJob.input.t_fdev_video_info"))
      .dateFormat()
      .filterVideosLatest()
    val output = videosInfoDf.join(channelsDf, Seq(VideoInfo.VideoId.name), "inner")
      .addColumns(
        CountryName(),
        Year()
      ).topLikes(top, year, country)

    output.filter(Trending.Year.column =!= year && Trending.Country.column =!= country &&
      Trending.CategoryEvent.column =!= "1").count() shouldBe 0
  }

  "topDislikes method" should "return a DataFrame with the list of videos top dislikes" in {
    val channelsDf: DataFrame = read(config.getConfig("YouTubeGlobalAwardsSparkJob.input.t_fdev_channels"))
    val videosInfoDf: DataFrame = read(config.getConfig("YouTubeGlobalAwardsSparkJob.input.t_fdev_video_info"))
      .dateFormat()
      .filterVideosLatest()
    val output = videosInfoDf.join(channelsDf, Seq(VideoInfo.VideoId.name), "inner")
      .addColumns(
        CountryName(),
        Year()
      ).topDislikes(top, year, country)

    output.filter(Trending.Year.column =!= year && Trending.Country.column =!= country &&
      Trending.CategoryEvent.column =!= "1").count() shouldBe 0
  }
}
