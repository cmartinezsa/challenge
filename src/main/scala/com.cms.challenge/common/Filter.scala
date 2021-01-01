package com.cms.challenge.common

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, date_sub, lit}


class Filter extends Atributos {
  val logger: Logger = LogManager.getLogger(msjLog)

  /**
    *
    * @param df       with the data for filter.
    * @param ticker   The name of tickers in a list.
    * @param days     Number of Days for operate
    * @param datePart Date base
    * @param spark    For Orchest the proccess.
    * @return DataFrame with the data filter.
    */
  def getFilteredOperations(df: DataFrame, ticker: List[String], days: Int, datePart: String)
                           (implicit spark: SparkSession): DataFrame = {
    val dfEmpty = spark.emptyDataFrame
    try {
      val datePointExtract = getDateToCompute(df, datePart, days)(spark)
      logger.info(s"Filter data by $datePointExtract and $datePart")

       df.filter(col(dateField) >= datePointExtract  && col(dateField)<=datePart)
        .filter(col(tickerField) isin (ticker: _*))

    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        dfEmpty
    }
  }

  /**
    *
    * @param df    with data for get value date.
    * @param date  Posicion date as reference
    * @param days  The numbers of days for substract to date.
    * @param spark for Orchest the process
    * @return String with the data found.
    */
  def getDateToCompute(df: DataFrame, date: String, days: Int)(implicit spark: SparkSession): String = {
    logger.info(s"Found start date  about $date  substract $days day(s)")
    val dateToExtract = df.select(date_sub(lit(date), days+1).alias("date_sub_start"))
      .select(col("date_sub_start"))
      .first().getDate(0).toString
    logger.info(s"Get date to extract data : " + dateToExtract)
    return dateToExtract
  }
}
