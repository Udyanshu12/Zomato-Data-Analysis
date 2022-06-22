package com.zomato.cleanUtil

import org.apache.spark.sql.functions.{col, explode, regexp_replace}
import org.apache.spark.sql.{DataFrame, functions}

trait CleaningZomato {

  def dropColumns(df: DataFrame): DataFrame = {
    val cols = Seq("url", "address", "phone", "reviews_list", "menu_item", "dish_liked", "listed_in(city)")
    val df1 = df.drop(cols: _*)
    df1
  }

    def newNames(df1:DataFrame):DataFrame={
      val newNames = Seq("id", "name", "onlineOrder", "bookTable", "rate", "votes", "location", "restType", "cuisines", "approxCost", "listedType")
      val dfRenamed = df1.toDF(newNames: _*)
      dfRenamed
    }

    def nullFilling(dfRenamed:DataFrame):DataFrame={
       dfRenamed.na.fill("0.0/5", Seq("rate"))
        .na.fill("null-Food", Seq("restType"))
        .na.fill(400, Seq("approxCost"))
        .na.fill("Delivery-null", Seq("listedType"))
    }


  def cleanRate(nullDF: DataFrame):DataFrame = {
    nullDF
      .withColumn("rateClean", regexp_replace(col("rate"), "/5", ""))
      .drop("rate")
  }

    def cuisineClean(cleanRate:DataFrame):DataFrame={
     cleanRate.withColumn("_cuisine", explode(functions.split(cleanRate("cuisines"), ",")))
       .drop("cuisines")
       .withColumn("cuisine", regexp_replace(col("_cuisine"), "\\s+", ""))
       .drop("_cuisine")
   }

  def restTypeClean(cuisineClean: DataFrame): DataFrame = {
    cuisineClean.withColumn("_restType", explode(functions.split(cuisineClean("restType"), ",")))
      .drop("restType")
      .withColumn("restType", regexp_replace(col("_restType"), "\\s+", ""))
      .drop("_restType")

  }

  def cleanDataFrame(dataframe: DataFrame): DataFrame = {
    val df = dropColumns(dataframe)
    val dfRenamed = newNames(df)
    val nullDF = nullFilling(dfRenamed)
    val cleanRate_ = cleanRate(nullDF)
    val cuisineClean_ = cuisineClean(cleanRate_)
    val restTypeClean_ = restTypeClean(cuisineClean_)

    restTypeClean_
  }

}