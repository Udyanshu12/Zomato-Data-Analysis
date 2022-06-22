package com.zomato.transformations

import com.zomato.Main.jdbcwriteDF
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Transformations {

  def maxConsumedCuisine(dataframe:DataFrame):DataFrame= {
    dataframe.groupBy("cuisine","rateClean", "name","approxCost").agg(count("cuisine").as ("maxConsumedCuisine")).orderBy(desc("maxConsumedCuisine"))
  }

  def minConsumedCuisine(dataframe:DataFrame):DataFrame= {
    dataframe.groupBy("cuisine","rateClean","name","approxCost").agg(count("cuisine").as ("minConsumedCuisine")).orderBy("minConsumedCuisine")
  }

  def maxRatedNorthIndianConsumedCuisine(dataframe: DataFrame, spark: SparkSession): DataFrame ={
    dataframe.createOrReplaceTempView("maxRateCuisine")
    spark.sql("""SELECT cuisine, name, votes, approxCost, rateClean FROM maxRateCuisine Where cuisine ='NorthIndian' ORDER BY votes DESC""")
  }

  def maxRatedVotedConsumedCuisine(dataframe: DataFrame, spark: SparkSession): DataFrame ={
    dataframe.createOrReplaceTempView("minRateCuisine")
    spark.sql("""SELECT cuisine, id, name, rateClean, votes FROM minRateCuisine ORDER BY votes DESC, rateClean DESC""")
  }

  def minApproxCostCuisine(dataframe: DataFrame, spark: SparkSession): DataFrame ={
    dataframe.createOrReplaceTempView("minApproxCost")
    spark.sql("""SELECT cuisine, id, name, approxCost, rateClean FROM minApproxCost WHERE cuisine = 'NorthIndian' AND approxCost IS NOT NULL ORDER BY approxCost ASC""")
  }


  def  maxVotedCostlyCuisine(dataframe: DataFrame, spark: SparkSession): DataFrame ={
    dataframe.createOrReplaceTempView("maxVotedCuisine")
    spark.sql("""SELECT cuisine, approxCost, votes, name FROM maxVotedCuisine ORDER BY  approxCost DESC, votes DESC, cuisine DESC, name DESC""")
  }

  def  minApproxRestType(dataframe: DataFrame, spark: SparkSession): DataFrame ={
    dataframe.createOrReplaceTempView("minApproxRestType_")
    spark.sql("""SELECT restType, id, name, approxCost, rateClean FROM minApproxRestType_ WHERE restType = 'FoodCourt' AND approxCost IS NOT NULL ORDER BY approxCost ASC""")
  }


  def  maxVoteCostlyRestType(dataframe: DataFrame, spark: SparkSession): DataFrame ={
    dataframe.createOrReplaceTempView("maxVoteRestType_")
    spark.sql("""SELECT restType, name, approxCost, rateClean, votes FROM maxVoteRestType_ WHERE restType = 'FoodCourt' ORDER BY approxCost DESC, votes DESC""")
  }


  def cuisineOfferedID(dataframe: DataFrame): DataFrame ={
    dataframe.groupBy("cuisine").agg(count("id").as("offeringRestaurants")).orderBy(desc("offeringRestaurants"))
  }

  def restTypeOfferedID(dataframe: DataFrame): DataFrame ={
    dataframe.groupBy("restType").agg(count("id").as("offeringRestaurants"))
  }


  def idWithMaxVotes(dataframe: DataFrame): DataFrame ={
    dataframe.select("id","rateClean","approxCost", "votes").orderBy(desc("votes"))
  }

  def maxOfferedRestType(dataframe: DataFrame): DataFrame ={
    dataframe.groupBy("restType").agg(count("id").as("offeringRestaurants")).orderBy(desc("offeringRestaurants"))
  }

  def maxRestaurantInLocation(dataframe: DataFrame): DataFrame ={
    dataframe.groupBy("location").agg(count("id").as("offeringRestaurants")
      , count(when(col("bookTable") === "Yes", 1)).as("tableBooking")
      , count(when(col("onlineOrder") === "Yes", 1)).as("onlineDelivery")).orderBy(desc("offeringRestaurants"))
  }

  def maxOnlineDelivery(dataframe: DataFrame): DataFrame ={
    dataframe.groupBy("location").agg(count("id").as("offeringRestaurants")
      , count(when(col("bookTable") === "Yes", 1)).as("tableBooking")
      , count(when(col("onlineOrder") === "Yes", 1)).as("onlineDelivery")).orderBy(desc("onlineDelivery"))
  }


  def maxTableBooking(dataframe: DataFrame): DataFrame ={
    dataframe.groupBy("location").agg(count("id").as("offeringRestaurants")
      , count(when(col("bookTable") === "Yes", 1)).as("tableBooking")
      , count(when(col("onlineOrder") === "Yes", 1)).as("onlineDelivery")).orderBy(desc("tableBooking"))
  }


  def nonOfferingOnlineDelivery(dataframe: DataFrame): DataFrame ={
    dataframe.groupBy("name", "location").agg(count("id").as("offeringRestaurants"),
      count(when(col("bookTable") === "Yes", 1)).as("tableBooking"),
      count(when(col("onlineOrder") === "Yes", 1)).as("onlineDelivery")).orderBy(asc("onlineDelivery"))
  }

  def approxRobin100(dataframe:DataFrame):DataFrame={
    dataframe.groupBy("name","id","approxCost").agg(count("name").as("nameCount")
    ,when(col("name") === "Robin Brown Ice Creams",col("approxCost")+100).as("approxUpdate")).where(col("name")==="Robin Brown Ice Creams")
  }

def approxAll100(dataframe:DataFrame):DataFrame={
  dataframe.groupBy("name","id","approxCost").agg(count("name").as("nameCount"))
    .withColumn("approxUpdate",col("approxCost")+100)
}

  def analysisDF(dataframe: DataFrame):Unit ={

    jdbcwriteDF(dataframe, "test_table")

    val maxConsumedCuisineDF = maxConsumedCuisine(dataframe)
    jdbcwriteDF(maxConsumedCuisineDF, "maxConsumedCuisine")

    val minConsumedCuisineDF = minConsumedCuisine(dataframe)
    jdbcwriteDF(minConsumedCuisineDF, "minConsumedCuisine")

    val cuisineOfferedIDDF = cuisineOfferedID(dataframe)
    jdbcwriteDF(cuisineOfferedIDDF, "cuisineOfferedID")

    val restTypeOfferedIDDF = restTypeOfferedID(dataframe)
    jdbcwriteDF(restTypeOfferedIDDF, "restTypeOfferedID")

    val idWithMaxVotesDF = idWithMaxVotes(dataframe)
    jdbcwriteDF(idWithMaxVotesDF, "idWithMaxVotes")

    val maxOfferedRestTypeDF = maxOfferedRestType(dataframe)
    jdbcwriteDF(maxOfferedRestTypeDF, "maxOfferedRestType")

    val maxRestaurantInLocationDF = maxRestaurantInLocation(dataframe)
    jdbcwriteDF(maxRestaurantInLocationDF, "maxRestaurantInLocation")

    val maxOnlineDeliveryDF = maxOnlineDelivery(dataframe)
    jdbcwriteDF(maxOnlineDeliveryDF, "maxOnlineDelivery")

    val maxTableBookingDF = maxTableBooking(dataframe)
    jdbcwriteDF(maxTableBookingDF, "maxTableBooking")

    val nonOfferingOnlineDeliveryDF = nonOfferingOnlineDelivery(dataframe)
    jdbcwriteDF(nonOfferingOnlineDeliveryDF, "nonOfferingOnlineDelivery")


  }

  def analysisSQL(dataframe: DataFrame, spark: SparkSession): Unit ={

    val maxRatedNorthIndianConsumedCuisineDF = maxRatedNorthIndianConsumedCuisine(dataframe, spark)
    jdbcwriteDF(maxRatedNorthIndianConsumedCuisineDF, "maxRatedNorthIndianConsumedCuisine")

    val maxRatedVotedConsumedCuisineDF = maxRatedVotedConsumedCuisine(dataframe, spark)
    jdbcwriteDF(maxRatedVotedConsumedCuisineDF, "maxRatedVotedConsumedCuisine")

    val minApproxCostCuisineDF = minApproxCostCuisine(dataframe, spark)
    jdbcwriteDF(minApproxCostCuisineDF, "minApproxCostCuisine")

    val maxVotedCostlyCuisineDF = maxVotedCostlyCuisine(dataframe, spark)
    jdbcwriteDF(maxVotedCostlyCuisineDF, "maxVotedCostlyCuisine")

    val minApproxRestTypeDF = minApproxRestType(dataframe, spark)
    jdbcwriteDF(minApproxRestTypeDF, "minApproxRestType")

    val maxVoteCostlyRestTypeDF = maxVoteCostlyRestType(dataframe, spark)
    jdbcwriteDF(maxVoteCostlyRestTypeDF, "maxVoteCostlyRestType")

  }

  def readAnalysis(dataframe: DataFrame): Unit ={

    val approxRobin100DF = approxRobin100(dataframe)
    jdbcwriteDF(approxRobin100DF, "updated_table")

    val approxAll100DF = approxAll100(dataframe)
    jdbcwriteDF(approxAll100DF, "approxAll100")
  }
}






















//workingDF.groupBy("name","id","approxCost").agg(count("name").as("nameCount"))
//.withColumn("approxUpdate",col("approxCost")+100).show()

//workingDF.select("id","name","approxCost").where(col("name")==="Robin Brown Ice Creams").withColumn("approxUpdate",col("approxCost")+100).show()
