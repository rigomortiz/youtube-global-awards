package com.datio.example.yga.commons.columns.input

import com.datio.example.yga.commons.columns.Col

object Channel {
  case object VideoId extends Col {
    override val name: String = "video_id"
  }

  case object ChannelTitle extends Col {
    override val name: String = "channel_title"
  }

  case object Title extends Col {
    override val name: String = "title"
  }

  case object PublishTime extends Col {
    override val name: String = "publish_time"
  }

  case object Tags extends Col {
    override val name: String = "tags"
  }

  case object Country extends Col {
    override val name: String = "country"
  }
}
