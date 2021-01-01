package com.cms.challenge.common

import org.apache.log4j.{LogManager, Logger}

object Args extends Atributos {
  val logger: Logger = LogManager.getLogger(msjLog)
  var target = ""

  /**
    *
    * @param args received
    * @return Booolean value
    */
  def getArguments(args: Array[String]): Boolean = {
    try {
     logger.info(s"Arguments recieved ${args.mkString(",")}")
     logger.info(s"Length Array recieved:  ${args.length}" )
      pathFile = args(0)
      datePart = args(1)
      tickerList = args(2).split(",").map(_.trim).toList
      numDay = args(3).toInt
      processName = args(4)
      modeExecution = args(5)
      target = args(6)

      logger.info(s"Parameters received => pathFile : $pathFile datePart: $datePart" +
        s" TickerList: $tickerList NumDay: $numDay ProcessName: $processName " +
        s" ModeExecution: $modeExecution  Target: $target")

      if (args.length < 7) {
        logger.info("Check number of params, required 7 arguments")
        checkParams
      }
      else {
        checkParams = true
        return checkParams
      }
    }
    catch {
      case e: ArrayIndexOutOfBoundsException =>
        logger.info("required 7 parameters " + e)
        println("required 7  parameters " + e)
        checkParams
      case _ =>
        logger.info(s"Check other case, Arguments recieved ${args.mkString(",")}")
        checkParams
    }
  }
}
