package com.cms.challenge.common

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

class Reader extends Atributos {
  val logger: Logger = LogManager.getLogger(msjLog)
  val filter = new Filter()
  val write = new Writer()


  /**
    *
    * @param pathFile from origin the dataset csv.
    * @param spark    Active
    * @return DataFrame with the data.
    */
  def readCSVFile(pathFile: String, datePart: String, ticker: List[String], numDay: Int)
                 (implicit spark: SparkSession): DataFrame = {
    val dfEmpty = spark.emptyDataFrame
    try {
      val df = spark.read
        .option("inferSchema", "true") // Make sure to use string version of true
        .option("header", true)
        .option("dateFormat", "yyyy-MM-dd")
        .option("sep", ",")
        .csv(pathFile)

      val dfFiltered = filter.getFilteredOperations(df, ticker, numDay, datePart)
        .persist()

      val dfCountRows = dfFiltered.count()
      logger.info(s"Total rows filtered $dfCountRows")
      dfFiltered
    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        dfEmpty
    }
  }

  def readParquetFile(pathFileParquet: String)(implicit spark: SparkSession): DataFrame = {
    val dfEmpty = spark.emptyDataFrame
    try {
      val df = spark
        .read
        .parquet(pathFileParquet)
      df
    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        dfEmpty
    }
  }
}
