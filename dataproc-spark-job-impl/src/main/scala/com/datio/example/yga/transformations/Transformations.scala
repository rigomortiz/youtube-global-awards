package com.datio.example.yga.transformations

import com.datio.example.yga.commons.Constants.{DATE, INT, LONG, ROW_NUMBER}
import com.datio.example.yga.commons.columns.input.{Channel, VideoInfo}
import com.datio.example.yga.commons.columns.output.Trending
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, date_format, lit, row_number, to_date}

object Transformations {
  implicit class ChannelDf(dataFrame: DataFrame) {
  }

  implicit class VideoInfoDf(dataFrame: DataFrame) {

    /**
     * Format date column from yy.dd.MM to yyyy-MM-dd
     * @return DataFrame with formatted date column
     */
    def dateFormat(): DataFrame = {
      dataFrame.select(
        dataFrame.columns.map(colName => {
          if (colName == VideoInfo.TrendingDate.name) {
            date_format(to_date(dataFrame(colName), "yy.dd.MM"), "yyyy-MM-dd")
              .cast(DATE)
              .alias(colName)
          } else {
            dataFrame(colName)
          }
        }): _*)
    }

    /**
      * Filter videos latest trending date
      *
      * @return a dataframe with the latest trending date
      */
    def filterVideosLatest(): DataFrame = {
      val rowNumberCol = col(ROW_NUMBER)
      val windowSpec = Window.partitionBy(VideoInfo.VideoId.column).orderBy(VideoInfo.TrendingDate.column.desc)
      dataFrame.select(dataFrame.columns.map(s => col(s)) :+ row_number().over(windowSpec).alias(ROW_NUMBER) :_*)
        .filter(rowNumberCol === 1)
        .distinct()
       .drop(rowNumberCol)
    }
  }

  implicit class TrendingDf(dataFrame: DataFrame) {

    /**
     * Add Columns
     * @param columns: Columns to add
     * @return DataFrame with new columns
     */
    def addColumns(columns: Column*): DataFrame = {
      columns.toList match {
        case Nil => dataFrame
        case h :: t => dataFrame.select(dataFrame.columns.map(col) :+ h: _*).addColumns(t: _*)
      }
    }

    /**
     * List of videos with more views by year, country limited to top
     * @param top: Number of videos to show
     * @param year: Year to filter
     * @param country: Country to filter
     * @return DataFrame with the list of videos
     */
    def topViews(top: Int, year: Int, country: String): DataFrame = {
      dataFrame
        .filter(Trending.Year.column === year && Trending.Country.column === country)
        .orderBy(Trending.Views.column.cast(INT).desc)
        .distinct()
        .limit(top)
        .select(dataFrame.columns.map(col) :+ lit("1").alias(Trending.CategoryEvent.name): _*)
    }

    /**
     * List of videos with more likes by year, country limited to top
     * @param top: Number of videos to show
     * @param year: Year to filter
     * @param country: Country to filter
     * @return DataFrame with the list of videos
     */
    def topLikes(top: Int, year: Int, country: String): DataFrame = {
      dataFrame
        .filter(Trending.Year.column === year && Trending.Country.column === country)
        .orderBy(Trending.Likes.column.cast(INT).desc)
        .distinct()
        .limit(top)
        .select(dataFrame.columns.map(col) :+ lit("2").alias(Trending.CategoryEvent.name) : _*)
    }

    /**
     * List of videos with more dislikes by year, country limited to top
     * @param top: Number of videos to show
     * @param year: Year to filter
     * @param country: Country to filter
     * @return DataFrame with the list of videos
     */
    def topDislikes(top: Int, year: Int, country: String): DataFrame = {
      dataFrame
        .filter(Trending.Year.column === year && Trending.Country.column === country)
        .orderBy(Trending.Dislikes.column.cast(INT).desc)
        .distinct()
        .limit(top)
        .select(dataFrame.columns.map(col) :+ lit("3").alias(Trending.CategoryEvent.name) : _*)
    }

    /**
     * Cast columns
     */
    def columnsCast(): DataFrame = {
      val cols: Array[Column] = dataFrame.columns.map {
        case colName@(Trending.Views.name | Trending.Likes.name | Trending.Dislikes.name) => col(colName).cast(LONG)
        case colName@Trending.CategoryId.name => col(colName).cast(INT)
        case c => col(c)
      }
      dataFrame.select(cols: _*).drop(Channel.PublishTime.column)
    }
  }
}
