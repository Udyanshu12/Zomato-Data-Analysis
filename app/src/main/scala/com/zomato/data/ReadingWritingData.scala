package com.zomato.data

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

trait ReadingWritingData {

  def dataframe(spark: SparkSession, _schema: StructType): DataFrame = {
    spark.read
      .option("header", "true")
      .schema(_schema)
      .option("delimiter", ",")
      .option("multiline", "true")
      .csv(
        "D:\\KSolves\\Zomato\\app\\src\\main\\resources\\csvFiles\\zomatas1.csv")
  }

  def jdbcwriteDF(dataframe: DataFrame, table: String): Unit = {
    dataframe.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/demo_db")
      .option("dbtable", table)
      .option("user", "root")
      .option("password", "root")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .mode(SaveMode.Overwrite)
      .save()
  }

  def jdbcReadDF(spark: SparkSession, table: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/demo_db")
      .option("dbtable", table)
      .option("user", "root")
      .option("password", "root")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .load()

  }
}
