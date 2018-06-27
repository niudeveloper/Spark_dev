package com.boy.spark

import java.util.Properties
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * 使用外部数据源综合查询Hive和MySQL的表数据
  */
object HiveMySQLApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("DataFromMysql").master("local[2]").getOrCreate()
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","admin")

    val dataFrame: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/exam","emp",properties)

    dataFrame.printSchema()

    dataFrame.show()

    spark.stop()
  }
}
