package com.datio.example.yga.commons

object Constants {
  val TYPE: String = s"type"
  val PATH: String = "path"
  val SCHEMA: String = s"schema.$PATH"
  val PATHS: String = s"paths"
  val DELIMITER: String = "delimiter"
  val HEADERS: String = "headers"
  val PARTITIONS: String = "partitions"
  val OPTIONS: String = "options"
  val INCLUDE_METADATA_FIELDS: String = s"$OPTIONS.includeMetadataFields"
  val PARTITION_OVERWRITE_MODE: String = s"$OPTIONS.partitionsOverwriteMode"
  val PARTITION_OVERWRITE_MODE_STRING: String = "partition_overwrite_mode_string"
  val INCLUDE_DELETED_FIELDS: String = s"$OPTIONS.includeDeletedFields"
  val COALESCE: String = s"$OPTIONS.coalesce"
  val MODE: String = "mode"
  //
  val JOB_NAME: String = "YouTubeGlobalAwardsSparkJob"
  val ROW_NUMBER: String = "row_number"
  val INT: String = "int"
  val LONG: String = "long"
  val DATE: String = "date"
  val PARAMETERS: String = "input.parameters"
}
