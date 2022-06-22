package com.zomato

import com.zomato.cleanUtil.CleaningZomato
import com.zomato.core.Session
import com.zomato.transformations.Transformations
import com.zomato.data.ReadingWritingData
import com.zomato.schema.Schema
import org.apache.log4j.{Level, Logger}

object Main extends Session with Schema with ReadingWritingData with CleaningZomato with Transformations {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = sparkSession
    val schema = _schema()

    val df = dataframe(spark, schema)

    val cleanDataframe = cleanDataFrame(df)
    analysisDF(cleanDataframe)
    analysisSQL(cleanDataframe, spark)

    val readDF = jdbcReadDF(spark, "test_table")
    readAnalysis(readDF)

    spark.stop()
  }
}
