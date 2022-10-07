Feature: Feature for YouTubeGlobalAwardsSparkJob
  Scenario: Test Engine should return 0 in success execution
    Given a config file with path: src/test/resources/config/application-test.conf
    When I execute the process: YouTubeGlobalAwardsSparkJob
    Then the exit code should be 0

  Scenario: Validate the output has only a country
    Given a dataframe outputDF in path: src/test/resources/data/output/t_fdev_trending
    When I filter outputDF dataframe with the filter: country <> "MX" and save it as outputFilteredDF dataframe
    Then outputFilteredDF dataframe has exactly 0 records

  Scenario: Validate the output has only a year
    Given a dataframe outputDF in path: src/test/resources/data/output/t_fdev_trending
    When I filter outputDF dataframe with the filter: year <> 2018 and save it as outputFilteredDF dataframe
    Then outputFilteredDF dataframe has exactly 0 records

  Scenario: Validate the output has only a number of videos
    Given a dataframe outputDF in path: src/test/resources/data/output/t_fdev_trending
    Then outputDF dataframe has exactly 30 records