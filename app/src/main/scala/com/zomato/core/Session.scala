package com.zomato.core

import org.apache.spark.sql.SparkSession

trait Session {

  def sparkSession: SparkSession = {
    SparkSession.builder
      .master("local[*]")
      .appName("Project")
      .getOrCreate()
  }

}
