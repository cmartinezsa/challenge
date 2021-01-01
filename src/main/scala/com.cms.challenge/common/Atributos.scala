package com.cms.challenge.common

trait Atributos {
  val saveModeAppend = "append"
  val saveModeOverwrite = "overwrite"
  val formatParquet = "parquet"
  val compresionSnappy = "snappy"
  val msjLog = "ETL - Data Pipeline - Challenge -"
  val tickerField = "ticker"
  val openValueField = "open"
  val closeValueField = "close"
  val adjCloseValueField = "adj_close"
  val lowValueField = "low"
  val highValueField = "high"
  val volumeValueField = "volume"
  val dateField = "date"
  val dateFormatField = "date_formated"
  var pathFile = ""
  var datePart = ""
  var numDay = 0
  var processName = ""
  var modeExecution = ""
  val formatDate = "yyyy-MM-dd"
  val castDate = "date"
  var checkParams = false
  var tickerList = List(toString)
  var dataDateProcess="data_date_process"
  var dateStartToExtract = "start_date"
  val innerJoin = "inner"
  val postgresPort = "spark.service.user.postgresql.port"
  val postgresHost = "spark.service.user.postgresql.host"
  val postgresUser = "spark.service.user.postgresql.user"
  val postgresPassword = "spark.service.user.postgresql.pass"
  val postgresDatabase = "spark.service.user.postgresql.database"

}
