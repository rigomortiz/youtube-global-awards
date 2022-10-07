package com.datio.example.yga

import io.cucumber.junit.{Cucumber, CucumberOptions}
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(
    features = Array("classpath:features"),
    glue = Array("com.datio.example.yga.steps"),
    strict = true,
    plugin = Array("pretty"))
class RunCukesTest
