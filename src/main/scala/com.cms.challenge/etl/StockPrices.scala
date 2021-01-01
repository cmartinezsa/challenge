package com.cms.challenge.etl

import com.cms.challenge.common.Args.target
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, current_timestamp}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.expressions.Window
import com.cms.challenge.common.{Atributos, DB, Reader, Writer}

class StockPrices extends Atributos {
  private val reader = new Reader()
  private val logger: Logger = LogManager.getLogger(msjLog)
  val db = new DB()
  val write = new Writer()

  /**
    *
    * @param pathFile
    * @param date_part
    * @param ticker
    * @param days
    * @param spark
    */
  def stockPricesETL(pathFile: String, date_part: String, ticker: List[String], days: Int)
                    (implicit spark: SparkSession): Unit = {
    logger.info("Parameters recieved : " + pathFile + " " + date_part + " " + ticker + " " + days)

    val emptyDF = spark.emptyDataFrame
    try {
      //Read the CSV File with contains historical_stock_prices
      val dfPrices = reader.readCSVFile(pathFile, date_part, ticker, days)

      // Create DataFrame with key indicator movil avg.
      val dfFinalMovilAvg = getJoinDFs(getMovAvgPrices(dfPrices))
        .withColumn("current_timestamp",current_timestamp()).as(dataDateProcess)
        .persist()
      if (!dfFinalMovilAvg.isEmpty) {
        val dfFinalMovilAvgCount=dfFinalMovilAvg.persist().count()
        //Prepare connection with Postgres for write into table.
        db.getDBConnection()
        write.saveResultJDBC(dfFinalMovilAvg, saveModeAppend, db.urlDB, db.user, db.password, target)
        logger.info(s"**** Write Sucessfull $dfFinalMovilAvgCount into ${db.urlDB}.$target ****")
      } else {
        logger.info("DataFrame not contains data")
      }
    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        emptyDF
    }
  }

  /**
    *
    * @param df
    * @param sparkSession
    * @return Five DataFrames with Column of moving average to key indicators.
    */
  def getMovAvgPrices(df: DataFrame)(implicit sparkSession: SparkSession): List[DataFrame] = {
    val movAvgOpen = df.select(col(tickerField), col(dateField), col(openValueField))
      .withColumn("movingAverageOpen", avg(col(openValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))

    val movAvgClose = df.select(col(tickerField), col(dateField), col(closeValueField))
      .withColumn("movingAverageClose", avg(col(closeValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))

    val movAvgAdjClose = df.select(col(tickerField), col(dateField), col(adjCloseValueField))
      .withColumn("movingAverageAdjClose", avg(col(adjCloseValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))

    val movAvgHigh = df.select(col(tickerField), col(dateField), col(highValueField))
      .withColumn("movingAverageHigh", avg(col(highValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))

    val movAvgLow = df.select(col(tickerField), col(dateField), col(lowValueField))
      .withColumn("movingAverageLow", avg(col(lowValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))

    val movAvgVolume = df.select(col(tickerField), col(dateField), col(volumeValueField))
      .withColumn("movingAverageVolume", avg(col(volumeValueField))
        .over(Window.partitionBy(tickerField).orderBy(dateField).rowsBetween(-1, 1)))

    logger.info("Created DataFrame Window about movAvgOpen, movAvgClose, movAvgAdjClose," +
      " movAvgHigh, movAvgVolume ")
    List(movAvgOpen, movAvgClose, movAvgAdjClose, movAvgHigh, movAvgLow, movAvgVolume)
  }

  /**
    *
    * @param dfs
    * @param spark
    * @return DataFrame with all join.
    */

  def getJoinDFs(dfs: List[DataFrame])(implicit spark: SparkSession): DataFrame = {
    val dfEmpty = spark.emptyDataFrame

    if (dfs.nonEmpty) {
      logger.info("DataFrame contains data")
      val dfFirst = dfs(0)
      val dfSecond = dfs(1)
      val dfThird = dfs(2)
      val dfFour = dfs(3)
      val dfFive = dfs(4)
      val dfSix = dfs(5)
      val dfJoinResultFirstPart = dfFirst.join(dfSecond, Seq(tickerField, dateField), innerJoin)
        .join(dfThird, Seq(tickerField, dateField), innerJoin)
      logger.info("Created dfJoinResultFirstPart")

      val dfJoinResultSecondPart = dfJoinResultFirstPart.join(dfFour, Seq(tickerField, dateField), innerJoin)
        .join(dfFive, Seq(tickerField, dateField), innerJoin)
        .join(dfSix, Seq(tickerField, dateField), innerJoin)
        .persist()
      logger.info("Created dfJoinResultSecondPart")

      dfJoinResultSecondPart
    } else {
      logger.info("Not found DFs to Join")
      dfEmpty
    }
  }
}
