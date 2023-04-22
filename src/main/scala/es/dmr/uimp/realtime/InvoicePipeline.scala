package es.dmr.uimp.realtime

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.HashMap

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import es.dmr.uimp.clustering.Clustering.{featurizeData, filterData, saveThreshold, toDataset}
import es.dmr.uimp.clustering.KMeansClusterInvoices
import es.dmr.uimp.clustering.KMeansClusterInvoices.{distToCentroid, trainModel}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object InvoicePipeline {

  case class Purchase(invoiceNo : String, quantity : Int, invoiceDate : String,
                      unitPrice : Double, customerID : String, country : String)

  case class Invoice(invoiceNo : String, avgUnitPrice : Double,
                     minUnitPrice : Double, maxUnitPrice : Double, time : Double,
                     numberItems : Double, lastUpdated : Long, lines : Int, customerId : String)




  def main(args: Array[String]) {

    //val Array(modelFile, thresholdFile, modelFileBisect, thresholdFileBisect, zkQuorum, group, topics, numThreads, brokers) = args
    val Array(zkQuorum, group, topics, numThreads, brokers) = args
    val sparkConf = new SparkConf().setAppName("InvoicePipeline")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(20))

    // Checkpointing
    ssc.checkpoint("./checkpoint")

    // TODO: Load model and broadcast
    //val (kMeansModel, kMeansThreshold) = loadKMeansAndThreshold(sc, modelFile, thresholdFile)
    //val kMeansBroadcast = sc.broadcast(kMeansModel)
    //val kMeansThresholdBroadcast = sc.broadcast(kMeansThreshold)

    // TODO: Build pipeline

    // connect to kafka
    val purchasesFeed = connectToPurchases(ssc, zkQuorum, group, topics, numThreads)

    purchasesFeed.print(5)
    // TODO: rest of pipeline

    val purchases = parsePurchases(purchasesFeed)
    System.out.println(purchases.toString)

    val validPurchases = getValidPurchases(purchases)
    System.out.println(validPurchases.toString)

    val wrongPurchases = getWrongPurchases(purchases)
    System.out.println(wrongPurchases.toString)

    val canceledPurchases = getCanceledPurchases(purchases)
    System.out.println(canceledPurchases.toString)

    //val predictions = predictAnomaly(sc, validPurchases, kMeansModel, kMeansThreshold)

/*
    predictions.foreachRDD(rdd => {
      publishToKafka("anomalias_kmeans")(sc.broadcast(brokers))(rdd)
    })

    wrongPurchases.foreachRDD(rdd => {
      publishToKafka("facturas_erroneas")(sc.broadcast(brokers))(rdd)
    })

    canceledPurchases.foreachRDD(rdd => {
      publishToKafka("cancelaciones")(sc.broadcast(brokers))(rdd)
    })
*/

    ssc.start() // Start the computation
    ssc.awaitTermination()


  }

  def publishToKafka(topic : String)(kafkaBrokers : Broadcast[String])(rdd : RDD[(String, String)]) = {
    rdd.foreachPartition( partition => {
      val producer = new KafkaProducer[String, String](kafkaConf(kafkaBrokers.value))
      partition.foreach( record => {
        producer.send(new ProducerRecord[String, String](topic, record._1,  record._2.toString))
      })
      producer.close()
    })
  }

  def kafkaConf(brokers : String) = {
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  /**
    * Load the model information: centroid and threshold
    */
  def loadKMeansAndThreshold(sc: SparkContext, modelFile : String, thresholdFile : String) : Tuple2[KMeansModel,Double] = {
    val kmeans = KMeansModel.load(sc, modelFile)
    // parse threshold file
    val rawData = sc.textFile(thresholdFile, 20)
    val threshold = rawData.map{line => line.toDouble}.first()

    (kmeans, threshold)
  }


  def connectToPurchases(ssc: StreamingContext, zkQuorum : String, group : String,
                         topics : String, numThreads : String): DStream[(String, String)] ={

    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
  }

  /**
   *
   * Own functions
   */

  def parsePurchases(stream: DStream[(String,String)]): DStream[(Purchase)] = {
    stream
      .map(_._2)
      .map(line => new CsvParser(new CsvParserSettings()).parseLine(line))
      .map(fields => Purchase(fields(0), fields(1).toInt, fields(2), fields(3).toDouble, fields(4), fields(5)))
  }

  def getValidPurchases(stream: DStream[Purchase]): DStream[Purchase] = {
    stream
      .filter(purchase =>
          purchase.quantity > 0 &&
          purchase.customerID != null &&
          purchase.invoiceDate != null &&
          !purchase.invoiceNo.startsWith("C"))
  }
  def getWrongPurchases(stream: DStream[Purchase]): DStream[(String, String)] = {
    stream
      .filter(purchase => purchase.quantity <= 0 || purchase.customerID == null || purchase.invoiceDate == null)
      .map(purchase => ("facturas_erroneas", 1.0))
      .reduceByKey(_ + _)
      .map(purchase => ("facturas_erroneas", purchase._2.toString))
  }

  def getCanceledPurchases(stream: DStream[Purchase]): DStream[(String, String)] = {
    stream
      .filter(purchase => purchase.invoiceNo.startsWith("C"))
      .countByWindow(Seconds(480), Seconds(20))
      .map(count => ("cancelaciones", count.toString))
  }
/*
  def predictAnomaly(sc: SparkContext, stream: DStream[Purchase], model: KMeansModel, threshold: Double): DStream[(String, String)] = {

    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val columns = List("InvoiceNo","Quantity","InvoiceDate","UnitPrice","CustomerID","Country")

    val dataframe = stream.foreachRDD(rdd => {
      spark.createDataFrame(rdd).toDF(columns:_*)
    })

    val featurized = featurizeData(dataframe)
    val filtered = filterData(featurized)
    val dataset = toDataset(filtered)

    // We are going to use this a lot (cache it)
    dataset.cache()

    val anomalies = dataset.filter(
      d => distToCentroid(d._1, model) > threshold
    )

    anomalies.count()
    anomalies.filter(x => x._2 != "normal.").count


    val count = stream.foreachRDD(rdd => {

      val dataframe = spark.createDataFrame(rdd).toDF(columns:_*)


      val featurized = featurizeData(dataframe)
      val filtered = filterData(featurized)
      val dataset = toDataset(filtered)

      // We are going to use this a lot (cache it)
      dataset.cache()

      val anomalies = dataset.filter(
        d => distToCentroid(d, model) > threshold
      )

      anomalies.count()
      anomalies.filter(x => x != "normal.").count
    })

    count


  }

  def updateState(newPurchase: Seq[Purchase], oldState: Option[(Purchase, String)]) = {

    val newState = oldState.getOrElse()

    val sortedPurchases = newPurchase.map(_.invoiceDate => LocalDateTime.parse(_.dateTime)).sortBy(_.invoiceDate)

    state match {
      case Some((prevPurchase, prevDate)) =>
        val elapsed = sortedPurchases.head.invoiceDate - prevPurchase.invoiceDate
        if (elapsed >= 40000) {
          Some(sortedPurchases.last, sortedPurchases.last.invoiceDate)
        } else {
          Some(prevPurchase, prevDate)
        }

      case None =>
        Some((sortedPurchases.last, sortedPurchases.last.invoiceDate))
    }

    }



  }
*/
}