package com.datio.example.yga.utils

import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.io.output.DatioDataFrameWriter
import com.datio.dataproc.sdk.schema.DatioSchema
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.datio.example.yga.commons.Constants._
import java.net.URI

trait IOUtils {
  lazy val datioSparkSession: DatioSparkSession = DatioSparkSession.getOrCreate()

  /**
   * Read a file from a path
   * @param inputConfig: Config file with the input path
   * @return DataFrame with the data
   */
  def read(inputConfig: Config): DataFrame = {
    val path: String = inputConfig.getStringList(PATHS).get(0)
    inputConfig.getString(TYPE) match {
      case "parquet" => datioSparkSession.read().parquet(path)
      case "csv" => {
        val schemaPath:String = inputConfig.getString(SCHEMA)
        val schema: DatioSchema = DatioSchema.getBuilder.fromURI(URI.create(schemaPath)).build()
        val delimiter: String = inputConfig.getString(DELIMITER)
        datioSparkSession.read()
          .option(HEADERS, true.toString)
          .option(DELIMITER, delimiter)
          .option("mode", "DROPMALFORMED")
          .datioSchema(schema)
          .csv(path)
      }
      case _@inputType => throw new Exception(s"Formato de archivo no soportado: $inputType")
    }
  }

  /**
   * Write a dataframe to a file
   * @param df: DataFrame to write
   * @param outputConfig: Config with the output configuration
   */
  def write(df: DataFrame, outputConfig: Config): Unit = {
    val mode: SaveMode = outputConfig.getString(MODE) match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case _@saveMode => throw new Exception(s"Modo de escritura no soportado: $saveMode")
    }

    val path: String = outputConfig.getString(PATH)

    val schemaPath: String = outputConfig.getString(SCHEMA)
    val includeMetadata: Boolean = outputConfig.getBoolean(INCLUDE_METADATA_FIELDS)
    val includeDeletedFields: Boolean = outputConfig.getBoolean(INCLUDE_DELETED_FIELDS)
    val coalesceNumber: Int = outputConfig.getInt(COALESCE)

    val schema: DatioSchema = DatioSchema.getBuilder
      .fromURI(URI.create(schemaPath))
      .withMetadataFields(includeMetadata)
      .withDeletedFields(includeDeletedFields)
      .build()

    val partitions: Array[String] = outputConfig.getStringList(PARTITIONS).toArray.map(_.toString)
    val partitionOverwriteMode: String = outputConfig.getString(PARTITION_OVERWRITE_MODE)

    val writer: DatioDataFrameWriter = datioSparkSession
      .write()
      .mode(mode)
      .option(PARTITION_OVERWRITE_MODE_STRING, partitionOverwriteMode)
      .datioSchema(schema)
      .partitionBy(partitions: _*)

    outputConfig.getString(TYPE) match {
      case "parquet" => writer.parquet(df.coalesce(coalesceNumber), path)
      case "csv" => writer.csv(df.coalesce(coalesceNumber), path)
      case "avro" => writer.avro(df.coalesce(coalesceNumber), path)
      case _@outputType => throw new Exception(s"Formato de escritura no soportado: $outputType")
    }

  }
}
