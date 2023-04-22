package es.dmr.uimp.clustering

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by root on 3/12/17.
  */
object Clustering {
  /**
    * Load data from file, parse the data and normalize the data.
    */
  def loadData(sc: SparkContext, file : String) : DataFrame = {
    val sqlContext = new SQLContext(sc)

    // Function to extract the hour from the date string
    val gethour =  udf[Double, String]((date : String) => {
      var out = -1.0
      if (!StringUtils.isEmpty(date)) {
        val hour = date.substring(10).split(":")(0)
        if (!StringUtils.isEmpty(hour))
          out = hour.trim.toDouble
      }
      out
    })

    // Load the csv data
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(file)
      .withColumn("Hour", gethour(col("InvoiceDate")))

    df
  }

  def featurizeData(df: DataFrame): DataFrame = {
    // TODO : Featurize the data

    val dfTotalPrices = df.withColumn("TotalPrice", df("Quantity")*df("UnitPrice"))

    // Prices and products by invoice
    val pricesProducts = df.groupBy("InvoiceNo").agg(
      avg("TotalPrice").alias("AvgUnitPrice"),
      min("TotalPrice").alias("MinUnitPrice"),
      max("TotalPrice").alias("MaxUnitPrice"),
      sum("Quantity").alias("NumberItems")
    )

    // Hour of the invoice
    val dfHour = dfTotalPrices.withColumn("Time", hour(df("InvoiceDate")))

    val featurizedDf = dfTotalPrices
      .join(pricesProducts, Seq("InvoiceNo"))
      .select("InvoiceNo", "AvgUnitPrice", "MinUnitPrice", "MaxUnitPrice", "Time", "NumberItems")

    featurizedDf
  }



  def filterData(df : DataFrame) : DataFrame = {
   // TODO: Filter cancelations and invalid
    val filteredDf = df
      .filter(col("Quantity")>0)
      .filter(col("CustomerID").isNotNull)
      .filter(col("InvoiceDate").isNotNull)
      .filter(!col("InvoiceNo").startsWith("C"))

    filteredDf
  }

  def toDataset(df: DataFrame): RDD[Vector] = {
    val data = df.select("AvgUnitPrice", "MinUnitPrice", "MaxUnitPrice", "Time", "NumberItems").rdd
      .map(row =>{
        val buffer = ArrayBuffer[Double]()
        buffer.append(row.getAs("AvgUnitPrice"))
        buffer.append(row.getAs("MinUnitPrice"))
        buffer.append(row.getAs("MaxUnitPrice"))
        buffer.append(row.getAs("Time"))
        buffer.append(row.getLong(4).toDouble)
        val vector = Vectors.dense(buffer.toArray)
        vector
      })

    data
  }

  def elbowSelection(costs: Seq[Double], ratio : Double): Int = {
    // TODO: Select the best model

    val ratios = costs.sliding(2).map{ case Seq(x, y) => y/x }.toSeq
    ratios.indexWhere(_ > ratio) match {
      case -1 => costs.size
      case i => i+1
    }

  }

  def saveThreshold(threshold : Double, fileName : String): Unit = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    // decide threshold for anomalies
    bw.write(threshold.toString) // last item is the threshold
    bw.close()
  }

}
