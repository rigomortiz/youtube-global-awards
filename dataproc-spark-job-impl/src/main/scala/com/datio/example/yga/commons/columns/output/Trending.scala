package com.datio.example.yga.commons.columns.output

import com.datio.example.yga.commons.columns.Col
import com.datio.example.yga.commons.columns.input.{Channel, VideoInfo}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, lit, when}

object Trending {
  case object Country extends Col {
    override val name: String = "country"
  }

  case object CountryName extends Col {
    override val name: String = "country_name"

    def apply(): Column = {
      when(Channel.Country.column === "CA", lit("Canada"))
        .when(Channel.Country.column === "DE", lit("Alemania"))
        .when(Channel.Country.column === "FR", lit("Francia"))
        .when(Channel.Country.column === "GB", lit("Gran Bretaña"))
        .when(Channel.Country.column === "IN", lit("India"))
        .when(Channel.Country.column === "JP", lit("Japon"))
        .when(Channel.Country.column === "KR", lit("Korea"))
        .when(Channel.Country.column === "MX", lit("Mexico"))
        .when(Channel.Country.column === "RU", lit("Rusia"))
        .when(Channel.Country.column === "US", lit("Estados Unidos")) otherwise "NULL" alias name
    }
  }

  case object VideoId extends Col {
    override val name: String = "video_id"
  }

  case object Year extends Col {
    override val name: String = "year"

    def apply(): Column = {
      VideoInfo.TrendingDate.column.substr(1, 4) cast "Int" alias name
    }
  }

  case object TrendingDate extends Col {
    override val name: String = "trending_date"
  }

  case object Title extends Col {
    override val name: String = "title"
  }

  case object ChannelTitle extends Col {
    override val name: String = "channel_title"
  }

  case object CategoryId extends Col {
    override val name: String = "category_id"
  }

  case object Tags extends Col {
    override val name: String = "tags"
  }

  case object Views extends Col {
    override val name: String = "views"
  }

  case object Likes extends Col {
    override val name: String = "likes"
  }

  case object Dislikes extends Col {
    override val name: String = "dislikes"
  }

  case object CategoryEvent extends Col {
    override val name: String = "category_event"

    def apply(): Column = {
      Window.partitionBy(Trending.Year.column).orderBy(VideoInfo.Views.column.desc)
        count(VideoInfo.VideoId.column) over Window.partitionBy(Trending.Year.column) alias name
    }
  }
}
