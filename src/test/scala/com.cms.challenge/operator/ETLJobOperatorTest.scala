package com.cms.challenge.operator

import com.cms.challenge.common.Args
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import com.cms.challenge.common.Reader

import org.apache.spark.sql.functions.{lit,datediff, max, min, col}

class ETLJobOperatorTest extends AnyFlatSpec with BeforeAndAfterAll {
  val reader=new Reader()
  var argumentsTestSucess: Array[String] = "resource/historical_stock_prices.csv 2015-12-30 AHH,APO,PEZ,CRCM,FLWS,GTN,GHDX,VIAV,MHD 365 AppChallengede local[*] hist_stock_prices_mov".split(" ")
  var argumentsTestFail: Array[String] = "resource/historical_stock_prices.csv 2015-12-31 AppChallengede local hist_stock_prices_mov".split(" ")

  val sparkSessionTest=ETLJobOperator.initializeSparkSession(argumentsTestSucess)

  val histStockPricesCSV="/src/test/scala/resource/historical_stock_prices.csv"

  override def beforeAll() {
    println("Inicio test")
  }
  it should "Validate sparkSession is sucessfull" in {
    val sparkAppName=sparkSessionTest.sparkContext.appName
    assert(sparkAppName != "Process Failed")
  }
  it should "compute the 7-day moving average for at least two stocks" in {
    val dfTest=reader.readCSVFile(histStockPricesCSV, "2015-12-30",Args.tickerList, 7)(sparkSessionTest)

   val dfTestSchema = dfTest
    dfTestSchema.printSchema()

    val numDaysToCompute= dfTest.select(datediff(lit(max(col("date"))), min(col("date"))))
      .first()
      .get(0).toString

    val numDaysToComputeMax= dfTest.select(lit(max(col("date"))))
        .first()
        .get(0).toString

    val numDaysToComputeMin= dfTest.select(lit(min(col("date"))))
      .first()
      .get(0).toString

    println(s"Valor max: $numDaysToComputeMax y el valor min: $numDaysToComputeMin")

    println("Valor obtenido de dias " + numDaysToCompute)
    assert(numDaysToCompute === "7")

    val tickerListTest= dfTest.select(col("ticker")).distinct()
        .rdd.map(r=>r(0)).collect().toList

    val tickerStringTest= tickerListTest.toString().split(",").sortWith(_<_)
    println("Valor de ticker string " + tickerStringTest)

    val tickerStringParam=  Args.tickerList.sortWith(_<_)

    println("Valor de ticker string " + tickerStringParam)

    println(s"Valores de $tickerListTest vs ${Args.tickerList}" )

    assert( tickerListTest.toString().split(",").sortWith(_<_) === Args.tickerList.sortWith(_<_))
  }
}
