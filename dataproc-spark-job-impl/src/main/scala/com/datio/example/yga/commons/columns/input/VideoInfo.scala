package com.datio.example.yga.commons.columns.input

import com.datio.example.yga.commons.columns.Col

object VideoInfo {
  case object VideoId extends Col {
    override val name: String = "video_id"
  }

  case object CategoryId extends Col {
    override val name: String = "category_id"
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

  case object TrendingDate extends Col {
    override val name: String = "trending_date"
  }

}
