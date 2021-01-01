package com.cms.challenge.operator

import com.cms.challenge.common.Atributos
import com.cms.challenge.etl.StockPrices
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

import com.cms.challenge.common.Args

object ETLJobOperator extends Atributos {
  private val LOGGER: Logger = LogManager.getLogger(msjLog)
  val STOCK_PRICES = new StockPrices()

  def main(args: Array[String]): Unit = {
   LOGGER.info("*** STARTING PROCESS *** ")
    val SPARK_SESSION = initializeSparkSession(args)
    if (Args.checkParams.equals(false)) {
      LOGGER.info("END PROCESS EARLY")
      SPARK_SESSION.stop()
    } else {
      LOGGER.info("VALIDATION OF PARAMETERS IS SUCESSFULL")
      STOCK_PRICES.stockPricesETL(Args.pathFile, Args.datePart, Args.tickerList, Args.numDay)(SPARK_SESSION)
      SPARK_SESSION.stop()
    }
    LOGGER.info("*** FINISHED ****")
  }

  /**
    *
    * @param args
    * @return SparkSession for orchestrate the process.
    */
  def initializeSparkSession(args: Array[String]): SparkSession = {
    LOGGER.info("Initializing SparkSession")
    Args.getArguments(args)
    if (Args.checkParams.equals(true)) {
      SparkSession
        .builder()
        .master(Args.modeExecution)
        .appName(Args.processName)
        .getOrCreate()
    }
    else {
      SparkSession
        .builder()
        .master(Args.modeExecution)
        .appName("Process Failed")
        .getOrCreate()
    }
  }
}

