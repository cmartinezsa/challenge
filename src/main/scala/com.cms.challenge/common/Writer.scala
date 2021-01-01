package com.cms.challenge.common

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame}

class Writer extends Atributos {
  val formatDateComplet = "dd/MM/yyyy HH:mm"
  val dateFormat = new SimpleDateFormat(formatDateComplet)
  val driver = "org.postgresql.Driver"
  val logger: Logger = LogManager.getLogger(msjLog)


  /**
    *
    * @param df
    * @param saveMode
    * @param format
    * @param ticker
    * @param date
    * @param pathToSave
    */
  def saveResultSetAsParquet(df: DataFrame, saveMode: String, format: String, ticker: String,
                             date: String, pathToSave: String): Unit = {
    try {
      df.write
        .mode(saveMode)
        .partitionBy(date, ticker)
        .option("compression", "snappy")
        .format(format)
        .save(pathToSave)
      logger.info("The Data save into the path : ".concat(pathToSave))
    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check table about $ex ")
    }
  }

  /**
    *
    * @param df
    * @param saveOption
    * @param jdbcUrl
    * @param jdbcUser
    * @param jdbcPassword
    * @param tableToWrite
    */
  def saveResultJDBC(df: DataFrame, saveOption: String, jdbcUrl: String,
                     jdbcUser: String, jdbcPassword: String,
                     tableToWrite: String): Unit = {
    val connectionProperties = new Properties
    connectionProperties.setProperty("driver", driver)
    connectionProperties.put("user", jdbcUser)
    connectionProperties.put("password", jdbcPassword)
    try {
      df.write
        .mode(saveOption)
        .jdbc(jdbcUrl, tableToWrite, connectionProperties)
      logger.info("Write to Postgres Sucessfull")
    }
    catch {
      case ex: IOException =>
        logger.error(s"Check the path about $ex ")
      case psql: org.postgresql.util.PSQLException =>
        logger.error(s"Check the database conecction $psql ")

    }
  }
  def saveResultCSV(df:DataFrame, pathToWrite:String, modeWrite:String): Unit ={
    try {
      df.coalesce(1)
        .write
        .format("csv")
        .option("header", "true")
        .mode(modeWrite)
        .save(pathToWrite)

      logger.info("The Data save into the path : ".concat(pathToWrite))
    }
      catch {
        case ex: AnalysisException =>
          logger.error(s"Check path about $ex ")
      }
    }
}

