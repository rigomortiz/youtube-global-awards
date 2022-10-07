
# Dataproc Spark Scala Job - YouTubeGlobalAwardsSparkJob

*This readme was generated by the project archetype, remove the next section and fill it with documentation* 

## Getting started with the new project
### First Compilation

Make a clean install in the root directory of the project

```bash
mvn clean install
```

### First Execution with the SparkLauncher of dataproc-sdk libraries

Go to your IDE run configurations window and set the following configuration:
 * Main class: `com.datio.example.yga.YouTubeGlobalAwardsSparkJobLauncher`
 * VM options: `-Dspark.master=local[*]`
 * Program arguments should be a valid path to a configuration file: `dataproc-spark-job-impl/src/test/resources/config/application-test.conf`
 * Working directory should point to the root path of the project: `/home/example/workspace/youtube-global-awards`
 * Use classpath of the main implementation module => `dataproc-spark-job-impl`
 * Set the location of the local input and output files as environment variables:
   * OUTPUT_SCHEMA_PATH = "dataproc-spark-job-impl/src/test/resources/schema/t_fdev_trending.output.schema"
   * INPUT_PATH = "dataproc-spark-job-impl/src/test/resources/data/input/parquet/youtube/t_fdev_channels"
   * INPUT_PATH = "dataproc-spark-job-impl/src/test/resources/data/input/parquet/youtube/t_fdev_video_info"
   * OUTPUT_PATH = "dataproc-spark-job-impl/src/test/resources/data/output/t_fdev_trending"

You will also need to enable the maven profile `run-local`.
