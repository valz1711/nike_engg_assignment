package com.cts.nike.file.proc.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window

object UniqueJSONWrite {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("COLLECT_LIST").master("local").getOrCreate()
    val input_path = args(0) //C:/IDW/Inputs/Nike/dataengineertest/dataengineertest/
    val output_path = args(1) //C:/IDW/Inputs/Nike/dataengineertest/dataengineertest/
    val sales_df = spark.read.format("csv").option("header", "true")
      .load(input_path + "sales.csv")
    val calendar_df = spark.read.format("csv").option("header", "true")
      .load(input_path + "calendar.csv")
    val product_df = spark.read.format("csv").option("header", "true")
      .load(input_path + "product.csv")
    val store_df = spark.read.format("csv").option("header", "true")
      .load(input_path + "store.csv")

    val join_df = sales_df.join(calendar_df, sales_df.col("dateId") === calendar_df.col("datekey"), "left")
      .join(product_df, product_df.col("productid") === sales_df.col("productId"), "left")
      .join(store_df, store_df.col("storeid") === sales_df.col("storeId"), "left")
      .withColumnRenamed("datecalendaryear", "year")
      .withColumn("unique_key", concat(
        col("year"), lit("_"),
        col("channel"), lit("_"),
        col("division"), lit("_"),
        col("gender"), lit("_"),
        col("category")))
      .select("unique_key", "division", "gender", "category", "channel", "year", "netSales", "salesUnits", "weeknumberofseason")

    val agg_df = join_df
      .groupBy("unique_key", "division", "gender", "category", "channel", "year", "weeknumberofseason")
      .agg(sum("netSales").alias("net_sales"), sum("salesUnits").alias("sales_units"))
      .withColumn("ZIP_VALUES", concat(col("weeknumberofseason"), lit("@@@"), col("net_sales"), lit("@@@"), col("sales_units")))
    val collect_df = agg_df.groupBy("unique_key", "division", "gender", "category", "channel", "year")
      .agg(collect_list("ZIP_VALUES").alias("COLLECT_ZIP"))
      .withColumn("_comment", lit("UniqueId needs to be created in your programme based on joining various csv files which we have provided. Year and Week will be available from calendar.csv. Channel will be available from store.csv. DIVISION, GENDER and CATEGORY will be available from product.csv"))

      def getDataRows(zipValues: Array[String]): Array[(String, String, scala.collection.mutable.Map[String, Double])] = {
      var dataRowsArray = new Array[(String, String, scala.collection.mutable.Map[String, Double])](2)
      var netSalesZip = new Array[Double](53)
      var salesUnitsZip = new Array[Double](53)
      val netSalesMapMut = scala.collection.mutable.Map[String, Double]()
      val salesUnitsMapMut = scala.collection.mutable.Map[String, Double]()
      for (zipValue <- zipValues) {
        val weekNum = zipValue.split("@@@")(0)
        val netSales = zipValue.split("@@@")(1).toDouble
        val salesUnits = zipValue.split("@@@")(2).toDouble
        netSalesZip(weekNum.toInt) = netSales
        salesUnitsZip(weekNum.toInt) = salesUnits
      }
      for (i <- 1 to 52) {
        if (netSalesZip(i) == null) {
          netSalesZip(i) = 0D
        }
        if (salesUnitsZip(i) == null) {
          salesUnitsZip(i) = 0D
        }
        netSalesMapMut += ("W" + i.toString() -> netSalesZip(i))
        salesUnitsMapMut += ("W" + i.toString() -> salesUnitsZip(i))
      }
      dataRowsArray(0) = ("Net Sales needs to be aggregated using transaction sales.csv data on weekly basis. If the value is not present for some week it needs to be filled with zero as given in below example",
        "Net Sales", netSalesMapMut)
      dataRowsArray(1) = ("Sales Unit needs to be aggregated using transactional sales.csv data on weekly basis. If the value is not present for some week it needs to be filled with zero as given in below example",
        "Sales Units", salesUnitsMapMut)

      return dataRowsArray
    }

    val udfDataRows = udf(getDataRows(_: Array[String]))
    val w = Window.orderBy("id")
    val final_df = collect_df
      .withColumn("dataRows", udfDataRows(col("COLLECT_ZIP")).cast("array<struct<_comment:string,rowId: string,dataRow: map<string,double>>>"))
      .select("_comment", "unique_key", "division", "gender", "category", "channel", "year", "dataRows")
      .withColumn("id", monotonically_increasing_id())
      .withColumn("index", row_number().over(w))

    final_df.select(col("_comment"), col("unique_key"), col("division"),
      col("gender"), col("category"), col("channel"), col("year"), col("dataRows"), col("index"))
      .write.format("json").partitionBy("index").mode("overwrite").save(output_path)///*"C:/IDW/Outputs/JSONExplode"*/)
  }
}