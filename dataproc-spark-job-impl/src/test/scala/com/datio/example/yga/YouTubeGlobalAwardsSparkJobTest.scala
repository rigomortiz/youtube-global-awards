package com.datio.example.yga

import com.datio.example.yga.context.{ContextProvider, FakeRuntimeContext}

class YouTubeGlobalAwardsSparkJobTest extends ContextProvider {

  "runProcess method" should "return 0 in success execution" in {
    val runtimeContext = new FakeRuntimeContext(config)
    val exitCode = new YouTubeGlobalAwardsSparkJob().runProcess(runtimeContext)
    exitCode shouldBe 0
  }

}