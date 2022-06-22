package com.zomato.schema

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

trait Schema {

  def _schema():StructType={
    StructType(List(
      StructField("id", IntegerType, nullable = true),
      StructField("url", StringType, nullable = true),
      StructField("address", StringType, nullable =  true),
      StructField("name", StringType, nullable = true),
      StructField("online_order", StringType, nullable = true),
      StructField("book_table", StringType, nullable = true),
      StructField("rate", StringType, nullable = true),
      StructField("votes", IntegerType, nullable = true),
      StructField("phone", StringType, nullable = true),
      StructField("location", StringType,  nullable = true),
      StructField("rest_type", StringType, nullable = true),
      StructField("dish_liked", StringType, nullable = true),
      StructField("cuisines", StringType, nullable = true),
      StructField("approx_cost(for two people)", IntegerType, nullable = true),
      StructField("reviews_list", StringType, nullable = true),
      StructField("menu_item", StringType, nullable = true),
      StructField("listed_in(type)", StringType, nullable = true),
      StructField("listed_in(city)", StringType, nullable = true)
    ))
  }

}
