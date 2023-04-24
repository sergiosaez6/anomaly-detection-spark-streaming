package es.dmr.uimp.realtime

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
import es.dmr.uimp.clustering.KMeansClusterInvoices.distToCentroid
import es.dmr.uimp.clustering.BisectionKMeansClusterInvoices.distToCentroid_bisect
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object InvoicePipeline {

  case class Purchase(invoiceNo : String, quantity : Int, invoiceDate : String,
                      unitPrice : Double, customerID : String, country : String)

  case class Invoice(invoiceNo : String, avgUnitPrice : Double,
                     minUnitPrice : Double, maxUnitPrice : Double, time : Double,
                     numberItems : Double, lastUpdated : Long, lines : Int, customerId : String)




  def main(args: Array[String]) {

    val Array(modelFile, thresholdFile, modelFileBisect, thresholdFileBisect, zkQuorum, group, topics, numThreads, brokers) = args
    //val Array(zkQuorum, group, topics, numThreads, brokers) = args
    val sparkConf = new SparkConf().setAppName("InvoicePipeline")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(20))

    // Checkpointing
    ssc.checkpoint("./checkpoint")

    // TODO: Load model and broadcast
    val (kMeansModel, kMeansThreshold) = loadKMeansAndThreshold(sc, modelFile, thresholdFile)
    val (bisectModel, bisectThreshold) = loadBisectKMeansAndThreshold(sc, modelFileBisect, thresholdFileBisect)
    val bcBrokers = sc.broadcast(brokers)

    // TODO: Build pipeline
    // connect to kafka
    val purchasesFeed = connectToPurchases(ssc, zkQuorum, group, topics, numThreads)

    System.out.println("Purchases received!")
    purchasesFeed.print()
    // TODO: rest of pipeline

    // First, the stream is parsed to a Purchase class stream
    // Followed by a stateful transformation to generate invoices
    val purchasesStream = parsePurchases(purchasesFeed)
    System.out.println("Purchases parsed")
    val invoicesStream = purchasesStream.updateStateByKey(updateStateFunc _)
    System.out.println("Invoices stream")

    // Once we have the proper invoices stream, we performed the required actions:
    // 1. Get the valid invoices, that will be predicted as anomalies or not afterwards
    // 2. Get the wrong invoices
    // 3. Get the canceled invoices
    val validInvoices = getValidInvoices(invoicesStream)
    System.out.println("Got valid invoices")

    val wrongInvoices = getWrongInvoices(invoicesStream)
    System.out.println("Extracted wrong invoices")

    val canceledInvoices = getCanceledInvoices(invoicesStream)
    System.out.println("Extracted canceled invoices")

    // Finally, all the information is published in the corresponding Kafka topic
    val predictionsKMeans = predictAnomalyKMeans(validInvoices, kMeansModel, kMeansThreshold)
    predictionsKMeans.foreachRDD(rdd => {
      publishToKafka("anomalias_kmeans")(bcBrokers)(rdd)
    })

    val predictionsBisectKMeans = predictAnomalyBisectKMeans(validInvoices, bisectModel, bisectThreshold)
    predictionsBisectKMeans.foreachRDD(rdd => {
      publishToKafka("anomalias_bisect_kmeans")(bcBrokers)(rdd)
    })

    wrongInvoices.foreachRDD(rdd => {
      publishToKafka("facturas_erroneas")(bcBrokers)(rdd)
    })

    canceledInvoices.foreachRDD(rdd => {
      publishToKafka("cancelaciones")(bcBrokers)(rdd)
    })


    ssc.start() // Start the computation
    ssc.awaitTermination()


  }

  /**
   * Publish the RDDs in the required topic
   */

  def publishToKafka(topic : String)(kafkaBrokers : Broadcast[String])(rdd : RDD[(String, String)]) = {
    rdd.foreachPartition( partition => {
      val producer = new KafkaProducer[String, String](kafkaConf(kafkaBrokers.value))
      partition.foreach( record => {
        producer.send(new ProducerRecord[String, String](topic, record._1,  record._2.toString))
      })
      producer.close()
    })
  }

  /**
   * Set up the proper Kafka configuration
   */
  def kafkaConf(brokers : String) = {
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  /**
    * Load the kmeans model information: centroid and threshold
    */
  def loadKMeansAndThreshold(sc: SparkContext, modelFile : String, thresholdFile : String) : Tuple2[KMeansModel,Double] = {
    val kmeans = KMeansModel.load(sc, modelFile)
    // parse threshold file
    val rawData = sc.textFile(thresholdFile, 20)
    val threshold = rawData.map{line => line.toDouble}.first()

    (kmeans, threshold)
  }

  /**
   * Connects to the required topic and retrieves the stream
   */
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

  /**
   *
   * Function to load the BisectingKMeans model
   */
  def loadBisectKMeansAndThreshold(sc: SparkContext, modelFile : String, thresholdFile : String) : Tuple2[BisectingKMeansModel,Double] = {
    val kmeans_bisect = BisectingKMeansModel.load(sc, modelFile)
    // parse threshold file
    val rawData = sc.textFile(thresholdFile, 20)
    val threshold = rawData.map{line => line.toDouble}.first()

    (kmeans_bisect, threshold)
  }

  /**
   *
   * Function to parse from the CSV string we receive to a Purchase object
   */
  def parsePurchases(stream: DStream[(String,String)]): DStream[(String,Purchase)] = {
    stream
      .map(_._2)
      .map(line => new CsvParser(new CsvParserSettings()).parseLine(line))
      .map(fields => (fields(0),Purchase(fields(0), fields(3).toInt, fields(4), fields(5).toDouble, fields(6), fields(7))))
  }

  /**
   *
   * Function to extract the hour from the date string
   */
  def getHour(date: String) : Double = {
    var out = -1.0
    if (!StringUtils.isEmpty(date)) {
      val hour = date.substring(10).split(":")(0)
      if (!StringUtils.isEmpty(hour))
        out = hour.trim.toDouble
    }
    out
  }
  /**
   *
   * Function to update the state of an invoice (from the received purchases), to be used in updateStateByKey()
   */
  def updateStateFunc(newPurchase: Seq[Purchase], currentInvoiceState: Option[Invoice]): Option[Invoice] = {
    // Set a control in case an empty Purchase is received
    if(newPurchase.isEmpty){
      // In this case, if there is a non-null state, it is necessary to check the SLA (40 seconds)
      if(currentInvoiceState!=null) {
        if(System.currentTimeMillis() - currentInvoiceState.get.lastUpdated < 40000) {
          currentInvoiceState
          // If not, the Invoice is deleted from memory setting its status to None
        } else {
          None
        }
      } else {
        currentInvoiceState
      }
    } else {
      val invoiceNo = newPurchase.head.invoiceNo
      // Aggregated values of the purchases
      val numberItems = newPurchase.map(_.quantity).sum
      val avgUnitPrice = newPurchase.map(_.unitPrice).sum/numberItems
      val minUnitPrice = newPurchase.map(_.unitPrice).min
      val maxUnitPrice = newPurchase.map(_.unitPrice).max
      val lastUpdated = System.currentTimeMillis()
      val time = getHour(newPurchase.head.invoiceDate)
      val lines = newPurchase.length
      val customerId = newPurchase.head.customerID

      // Get the current state to 'initialize' the new one. getOrElse allows to get the state in case it exists
      // or create a new one with the Invoice's arguments
      val newState = currentInvoiceState.getOrElse(Invoice(invoiceNo,avgUnitPrice,minUnitPrice,maxUnitPrice,time,numberItems,
        lastUpdated,lines,customerId))

      // If it has been less than 40 seconds (40000 milliseconds) we just update the current state with the
      // aggregated values from the incoming purchases
      if(lastUpdated - newState.lastUpdated < 40000) {
        Some(newState.copy(avgUnitPrice=avgUnitPrice, minUnitPrice=minUnitPrice, maxUnitPrice=maxUnitPrice,
          numberItems=newState.numberItems+numberItems, lastUpdated=lastUpdated, lines=newState.lines+lines))
        // If not, the Invoice is deleted from memory setting its status to None
      } else {
        None
      }
    }

  }

  /**
   *
   * Function to get only those valid invoices:
   * - No negative number of items
   * - Non-null customer
   * - Non-null invoice date/hour (time)
   * - Non-canceled invoices
   */
  def getValidInvoices(stream: DStream[(String,Invoice)]): DStream[(String,Invoice)] = {
    stream
      .filter(invoice =>
          invoice._2.numberItems > 0 &&
          invoice._2.customerId != null &&
          invoice._2.time != null &&
          !invoice._2.invoiceNo.startsWith("C"))
  }

  /**
   *
   * Function to the those non-valid invoices
   */
  def getWrongInvoices(stream: DStream[(String,Invoice)]): DStream[(String, String)] = {
    stream
      .filter(invoice => invoice._2.numberItems <= 0 || invoice._2.customerId == null || invoice._2.time == null)
      .map(invoice => (invoice._1, "The invoice with invoiceNo: "+invoice._1+" is not valid"))
  }

  /**
   *
   * Function to get the canceled invoices
   */
  def getCanceledInvoices(stream: DStream[(String,Invoice)]): DStream[(String, String)] = {
    stream
      .filter(invoice => invoice._1.startsWith("C"))
      .countByWindow(Seconds(480), Seconds(60))
      .map(count => (count.toString,"Canceled invoices in the last 8 minutes: "+count.toString))
  }

  /**
   *
   * Function to use the KMeansModel algorithm to predict whether the incoming invoice is an anomaly or not
   */
  def predictAnomalyKMeans(stream: DStream[(String,Invoice)], model: KMeansModel, threshold: Double): DStream[(String, String)] = {

    val dataset = stream.map{ case (invoiceNo, invoice) =>
      (invoiceNo, Vectors.dense(
        invoice.avgUnitPrice,
        invoice.minUnitPrice,
        invoice.maxUnitPrice,
        invoice.time,
        invoice.numberItems
      ))
    }

    dataset.cache()

    val anomalies = dataset.filter(
      d => distToCentroid(d._2, model) > threshold
    ).map(tuple => (tuple._1,"The invoice with invoiceNo: "+tuple._1+" is considered an anomaly by KMeans"))

    anomalies

  }

  /**
   *
   * Function to used the BisectingKMeansModel algorithm to predict whether the incoming invoice is an anomaly or not
   */
  def predictAnomalyBisectKMeans(stream: DStream[(String,Invoice)], model: BisectingKMeansModel, threshold: Double): DStream[(String, String)] = {

    val dataset = stream.map{ case (invoiceNo, invoice) =>
      (invoiceNo, Vectors.dense(
        invoice.avgUnitPrice,
        invoice.minUnitPrice,
        invoice.maxUnitPrice,
        invoice.time,
        invoice.numberItems
      ))
    }

    dataset.cache()

    val anomalies = dataset.filter(
      d => distToCentroid_bisect(d._2, model) > threshold
    ).map(tuple => (tuple._1,"The invoice with invoiceNo: "+tuple._1+" is considered an anomaly by BisectingKMeans"))


    anomalies

  }

}